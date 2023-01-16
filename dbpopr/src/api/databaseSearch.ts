import axios from "axios";
import {TableName} from "../models/TableName";

export interface SearchTableResult {
 displayName: string;
 tableName: TableName;
 columns: string[];
 searches: SearchTableSearchBy[];
}

export interface SearchTableSearchBy {
 displayName: string;
 columns: string[];
}

export function databaseSearch(query: string) {
    return axios.get<SearchTableResult[]>('/database/search', {params: {query}})
}