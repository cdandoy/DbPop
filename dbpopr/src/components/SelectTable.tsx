import {Option} from "react-bootstrap-typeahead/types/types";
import React, {useState} from "react";
import axios from "axios";
import {AsyncTypeahead} from "react-bootstrap-typeahead";
import {TableName} from "../models/TableName";
import {Dependency} from "../models/Dependency";

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

export function SelectTable(props: any) {
    // Cheating with the type. See https://github.com/ericgio/react-bootstrap-typeahead/issues/738
    const setTableSelections: ((s: Option[]) => void) = props['setTableSelections'];
    const setDependency: ((s: Dependency) => void) = props['setDependency'];
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
                setTableSelections(selections);
                if (selections.length) {
                    let searchTableResult = selections[0] as SearchTableResult;
                    const root: Dependency = {
                        displayName: 'root',
                        tableName: searchTableResult.tableName,
                        constraintName: null,
                        subDependencies: null,
                        selected: true,
                        mandatory: true
                    };
                    setDependency(root);
                }
            }
            }
        />
    )
}