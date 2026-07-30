export interface IResponsePages
{
    current: number;
    hasNext: boolean;
    hasPrev: boolean;
    next: number;
    prev: number;
    total_pages: number;
    total_items: number;
}

export interface IPaginatedResult<T>
{
    data: T[];
    pages: IResponsePages;
}
