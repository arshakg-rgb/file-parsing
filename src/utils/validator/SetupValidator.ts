import { ArraySchema, ObjectSchema, ValidationResult } from "joi";

/**
 * Validates an object against a given Joi schema.
 *
 * @param obj - The object to validate.
 * @param schema - The Joi schema to validate against.
 * @param stripUnknown - Whether to strip unknown keys from the object.
 * @param allowUnknown - Whether to allow unknown keys in the object.
 * @returns The result of the validation.
 */
export function setupValidator(obj: {}, schema: ObjectSchema | ArraySchema, stripUnknown: boolean = true, allowUnknown: boolean = true): ValidationResult
{
    return schema.validate(obj, { stripUnknown, allowUnknown });
}
