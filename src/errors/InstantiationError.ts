import { CustomError } from "./CustomError.js";

export class InstantiationError extends CustomError 
{
  constructor(message: string = "Cannot instantiate singleton directly") 
{
    super(message, "INSTANTIATION_ERROR", 500);
  }
}
