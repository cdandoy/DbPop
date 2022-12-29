import {TableName} from "../models/TableName";

export interface Dependency {
    displayName: string;
    tableName: TableName,
    constraintName: string | null,
    subDependencies: Dependency[] | null,
    selected: boolean,
    mandatory: boolean,
}