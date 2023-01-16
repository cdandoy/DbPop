import {Option} from "react-bootstrap-typeahead/types/types";
import React, {useState} from "react";
import axios from "axios";
import {AsyncTypeahead} from "react-bootstrap-typeahead";
import {Table} from "../models/Table";
import {databaseSearch, SearchTableResult} from "../api/databaseSearch";

export function SelectTable({setTable}: { setTable: (table: Table) => void }) {
    const [isLoading, setIsLoading] = useState(false);
    const [tables, setTables] = useState<SearchTableResult[]>([]);

    const handleSearch = (query: string) => {
        setIsLoading(true);

        databaseSearch(query)
            .then(result => {
                setTables(result.data);
                setIsLoading(false);
            })
    };

    function whenTableSelected(selections: Option[]) {
        let selection = selections.length ? selections[0] : null;
        if (selection) {
            let searchTableResult = selection as SearchTableResult;
            let tableName = searchTableResult.tableName;
            axios.get<Table>(`/database/tables/${tableName.catalog}/${tableName.schema}/${tableName.table}`).then(result => {
                let table = result.data;
                if (table) {
                    setTable(table);
                }
            });
        }
    }

    // Bypass client-side filtering by returning `true`. Results are already
    // filtered by the search endpoint, so no need to do it again.
    const filterBy = () => true;
    return (
        <AsyncTypeahead
            filterBy={filterBy}
            id="async-table"
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