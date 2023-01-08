import {TableName} from "./TableName";
import {PrimaryKey} from "./PrimaryKey";
import {ForeignKey} from "./ForeignKey";

export interface Column {
    name: string;
    nullable: boolean;
    autoIncrement: boolean;
}

export interface Index {
    name: string;
    tableName: TableName;
    unique: boolean;
    primaryKey: PrimaryKey | null;
    columns: string[];
}

export interface Table {
    tableName: TableName;
    columns: Column[];
    indexes: Index[];
    primaryKey: PrimaryKey | null;
    foreignKeys: ForeignKey[];
}