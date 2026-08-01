export interface QueueMessage<T> {
    payload: T;
    receiptHandle: string;
}
