import {Option} from "react-bootstrap-typeahead/types/types";
import React, {useState} from "react";
import axios from "axios";
import {AsyncTypeahead} from "react-bootstrap-typeahead";
import {TableName} from "../models/TableName";

export interface SearchTableResult {
    displayName: string;
    tableName: TableName;
    columns: string[];
    searches: SearchTableSearchBy[];
}

interface SearchTableSearchBy {
    displayName: string;
    columns: string[];
}

interface TableResponse {
    tableName: TableName,
    columns: string[],
}

export function SelectTable(props: any) {
    // Cheating with the type. See https://github.com/ericgio/react-bootstrap-typeahead/issues/738
    const setTableSelection: ((s: Option | null) => void) = props['setTableSelection'];
    const setSelection: ((tableName: TableName, columns: string[]) => void) = props['setSelection'];
    const [isLoading, setIsLoading] = useState(false);
    const [tables, setTables] = useState<SearchTableResult[]>([]);

    const handleSearch = (query: string) => {
        setIsLoading(true);

        axios.get<SearchTableResult[]>('/database/search', {params: {query}})
            .then(result => {
                setTables(result.data);
                setIsLoading(false);
            })
    };

    function whenTableSelected(selections: Option[]) {
        let selection = selections.length ? selections[0] : null;
        if (selection && setSelection) {
            let searchTableResult = selection as SearchTableResult;
            let tableName = searchTableResult.tableName;
            axios.get<TableResponse>(`/database/tables/${tableName.catalog}/${tableName.schema}/${tableName.table}`).then(result => {
                let tableResponse = result.data;
                if (tableResponse) {
                    setSelection(tableResponse.tableName, tableResponse.columns);
                }
            });
        }
        if (setTableSelection) {
            setTableSelection(selection);
        }
    }

    // Bypass client-side filtering by returning `true`. Results are already
    // filtered by the search endpoint, so no need to do it again.
    const filterBy = () => true;
    return (
        <AsyncTypeahead
            filterBy={filterBy}
            id="async-example"
            isLoading={isLoading}
            minLength={3}
            onSearch={handleSearch}
            options={tables}
            labelKey={"displayName"}
            placeholder="Search..."
            inputProps={{
                autoCorrect: 'off', // Safari-only
                spellCheck: false,
            }}
            onChange={selections => {
                whenTableSelected(selections);
            }}
        />
    )
}