import {TableName} from "./TableName";

export interface Dependency {
    displayName: string;
    tableName: TableName,
    constraintName: string | null,
    subDependencies: Dependency[] | null,
    selected: boolean,
    mandatory: boolean,
}