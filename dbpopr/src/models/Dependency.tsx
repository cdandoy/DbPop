import {TableName, tableNameEquals} from "./TableName";

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

export function dependencyEquals(d1: Dependency, d2: Dependency) {
    return d1.constraintName === d2.constraintName && tableNameEquals(d1.tableName, d2.tableName);
}

export function searchDependency(root: Dependency, search: Dependency): Dependency | undefined {
    if (dependencyEquals(root, search)) return root;
    if (root.subDependencies) {
        for (const subDependency of root.subDependencies) {
            const found = searchDependency(subDependency, search);
            if (found) {
                return found;
            }
        }
    }
}
