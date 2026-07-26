import type { Selectable } from "kysely";
import type { AppDatabase, BookingTable } from "../db";
import { jsonError } from "../errors";

interface CreateBookingBody {
  userId?: unknown;
  slotId?: unknown;
  idempotencyKey?: unknown;
}

type Booking = Selectable<BookingTable>;

/** Domain error that maps cleanly to an API error response. */
class BookingBusinessError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "BookingBusinessError";
  }
}

/** Raised when two requests race on the same idempotency key. */
class IdempotencyRaceError extends Error {
  constructor() {
    super("Concurrent idempotent booking");
    this.name = "IdempotencyRaceError";
  }
}

/** Map a DB booking row to the API success payload. */
function serializeBooking(row: Booking) {
  return {
    id: row.id,
    userId: row.user_id,
    slotId: row.slot_id,
    status: row.status,
  };
}

/** Return 201 Created with the serialized booking. */
function bookingCreated(row: Booking): Response {
  return Response.json(serializeBooking(row), { status: 201 });
}

/** Parse JSON body; return null when the payload is invalid. */
async function readBody(request: Request): Promise<CreateBookingBody | null> {
  try {
    return (await request.json()) as CreateBookingBody;
  } catch {
    return null;
  }
}

/** Detect a SQLite UNIQUE constraint failure for a given column hint. */
function isUniqueViolation(error: unknown, columnHint: string): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }

  const candidate = error as { code?: string; message?: string };
  return (
    candidate.code === "SQLITE_CONSTRAINT_UNIQUE" &&
    typeof candidate.message === "string" &&
    candidate.message.includes(columnHint)
  );
}

/** Look up an existing booking by idempotency key. */
async function findBookingByIdempotencyKey(
  db: AppDatabase,
  idempotencyKey: string,
): Promise<Booking | undefined> {
  return db
    .selectFrom("bookings")
    .selectAll()
    .where("idempotency_key", "=", idempotencyKey)
    .executeTakeFirst();
}

/**
 * Replay a prior booking when the key matches the same payload;
 * otherwise reject key reuse with a different user/slot.
 */
function resolveExistingIdempotentBooking(
  existing: Booking,
  userId: number,
  slotId: number,
): Response {
  if (existing.user_id === userId && existing.slot_id === slotId) {
    return bookingCreated(existing);
  }

  return jsonError(
    409,
    "IDEMPOTENCY_KEY_CONFLICT",
    "Idempotency key was already used with a different booking",
  );
}

/** Create a booking atomically, with idempotency and capacity checks. */
export async function handleCreateBookingRequest(
  request: Request,
  db: AppDatabase,
): Promise<Response> {
  const body = await readBody(request);
  const { userId, slotId, idempotencyKey: rawIdempotencyKey } = body ?? {};

  if (
    !Number.isInteger(userId) ||
    !Number.isInteger(slotId) ||
    typeof rawIdempotencyKey !== "string" ||
    rawIdempotencyKey.trim().length === 0
  ) {
    return jsonError(
      400,
      "VALIDATION_ERROR",
      "userId, slotId and idempotencyKey are required",
    );
  }

  const typedUserId = userId as number;
  const typedSlotId = slotId as number;
  const idempotencyKey = rawIdempotencyKey.trim();

  // Fast path: same key + same payload returns the existing booking.
  const existingByKey = await findBookingByIdempotencyKey(db, idempotencyKey);
  if (existingByKey) {
    return resolveExistingIdempotentBooking(
      existingByKey,
      typedUserId,
      typedSlotId,
    );
  }

  const user = await db
    .selectFrom("users")
    .select("id")
    .where("id", "=", typedUserId)
    .executeTakeFirst();

  if (!user) {
    return jsonError(404, "USER_NOT_FOUND", "User was not found");
  }

  try {
    const booking = await db.transaction().execute(async (trx) => {
      // Reserve one seat only if capacity remains (safe under concurrency).
      const updateResult = await trx
        .updateTable("slots")
        .set(({ eb }) => ({
          remaining: eb("remaining", "-", 1),
        }))
        .where("id", "=", typedSlotId)
        .where("remaining", ">", 0)
        .executeTakeFirst();

      if (updateResult.numUpdatedRows === 0n) {
        const slot = await trx
          .selectFrom("slots")
          .select("id")
          .where("id", "=", typedSlotId)
          .executeTakeFirst();

        if (!slot) {
          throw new BookingBusinessError(
            404,
            "SLOT_NOT_FOUND",
            "Slot was not found",
          );
        }

        throw new BookingBusinessError(409, "SLOT_FULL", "Slot is fully booked");
      }

      try {
        return await trx
          .insertInto("bookings")
          .values({
            user_id: typedUserId,
            slot_id: typedSlotId,
            idempotency_key: idempotencyKey,
          })
          .returningAll()
          .executeTakeFirstOrThrow();
      } catch (error) {
        // Concurrent retry with the same key — roll back and replay outside.
        if (isUniqueViolation(error, "idempotency_key")) {
          throw new IdempotencyRaceError();
        }

        if (
          isUniqueViolation(error, "bookings.user_id") ||
          isUniqueViolation(error, "bookings.slot_id")
        ) {
          throw new BookingBusinessError(
            409,
            "DUPLICATE_BOOKING",
            "User already has a booking for this slot",
          );
        }

        throw error;
      }
    });

    return bookingCreated(booking);
  } catch (error) {
    if (error instanceof BookingBusinessError) {
      return jsonError(error.status, error.code, error.message);
    }

    if (error instanceof IdempotencyRaceError) {
      const raced = await findBookingByIdempotencyKey(db, idempotencyKey);
      if (raced) {
        return resolveExistingIdempotentBooking(raced, typedUserId, typedSlotId);
      }

      return jsonError(500, "INTERNAL_ERROR", "Unexpected booking error");
    }

    return jsonError(500, "INTERNAL_ERROR", "Unexpected booking error");
  }
}