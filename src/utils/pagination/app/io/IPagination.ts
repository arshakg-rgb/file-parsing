export interface IPaginationParams
{
    page?: number;
    limit?: number;
    sortBy?: string;
    fromCache?: boolean;
    sortOrder?: "ASC" | "DESC";
    search_value?: string;
}

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

export interface IFetchParams
{
    params: {};
    defaultSortBy?: string;
    searchFields?: string[];
}
