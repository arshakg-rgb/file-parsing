export class ApiError extends Error {
    constructor(
        message: string,
        public status: number,
        public code?: string,
        public details?: Record<string, any>
    ) {
        super(message);
        this.name = "ApiError";
    }
}
