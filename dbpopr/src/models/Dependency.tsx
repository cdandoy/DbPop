import {TableName} from "./TableName";

export interface Query {
    column: string,
    value: string,
}

export interface Dependency {
    displayName: string;
    tableName: TableName,
    constraintName: string | null,
    subDependencies: Dependency[] | null,
    selected: boolean,
    mandatory: boolean,
    queries?: Query[],
}