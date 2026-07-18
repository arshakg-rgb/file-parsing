/**
 * Field/column/key synonyms so field_spec names match real-world headers and JSON keys.
 */
export const FIELD_ALIASES: Record<string, string[]> = {
  email: ["email", "mail", "emailaddress", "e_mail", "email_address", "EmailAddress"],
  name: ["name", "fullname", "full_name", "firstName", "lastName", "first_name", "last_name"],
  phone: ["phone", "mobile", "telephone", "phonenumber", "msisdn", "phone_number", "phoneNumber"],
  address: ["address", "addr", "streetaddress", "street_address", "fullAddress"],
  zip: ["zip", "zipcode", "zip_code", "postal", "postalcode", "postal_code"],
  city: ["city", "town", "locality"],
  state: ["state", "province", "region"],
  country: ["country", "nation", "country_code"],
  date: ["date", "datetime", "timestamp", "created_at", "updated_at"],
  id: ["id", "identifier", "user_id", "customer_id"],
  username: ["username", "user_name", "userid", "user"],
};

/**
 * Delimiters to try for CSV-like parsing
 */
export const DELIMITERS = [",", ";", "\t", "|", "~"];

/**
 * Maximum line length for processing
 */
export const MAX_LINE_LENGTH = 64 * 1024;

/**
 * Binary content threshold (ratio of non-printable characters)
 */
export const BINARY_THRESHOLD = 0.3;

/**
 * Minimum fields required for header detection
 */
export const MIN_HEADER_FIELDS = 2;

/**
 * Minimum header match ratio (must match at least half of requested fields)
 */
export const HEADER_MATCH_RATIO = 0.5;

/**
 * Phone number digit range for validation
 */
export const PHONE_MIN_DIGITS = 10;
export const PHONE_MAX_DIGITS = 15;

/**
 * Template IDs for special cases
 */
export const TEMPLATE_IDS = {
  LENGTH_GATE: "length-gate",
  BINARY_GATE: "binary-gate",
  HEADER: "header",
  JSON: "json",
  KV: "kv",
  CSV_MAPPED: "csv-mapped",
  CSV_AUTO: "csv-auto",
} as const;
