import React from "react";

export default function SelectedColumns({allTableColumns, selectedColumns, setSelectedColumns, matchColumns}: {
    allTableColumns: string[] | null,
    selectedColumns: string[],
    setSelectedColumns: (s: string[]) => void,
    matchColumns: string[] | undefined | null;
}) {
    if (!allTableColumns) return <></>;

    let rows: string[] = [];
    if (!matchColumns) {
        rows = selectedColumns.filter(it => it.length).concat(['']);
    } else {
        selectedColumns = selectedColumns.slice(0, matchColumns.length);
        matchColumns = matchColumns.filter(it => it.length);
        while (rows.length < matchColumns.length && rows.length < selectedColumns.length) {
            rows.push(selectedColumns[rows.length]);
        }
        while (rows.length < matchColumns.length) {
            rows.push('');
        }
    }
    const availableColumns = [''].concat(allTableColumns);

    return (
        <>
            {rows.map((selectedColumn, i) => {
                return (
                    <div key={i} className={"ms-4 mt-2"}>
                        <select className="form-select"
                                aria-label="Column"
                                defaultValue={selectedColumn}
                                onChange={event => {
                                    rows[i] = event.target.value;
                                    // rows = rows.filter(it => it.length);
                                    setSelectedColumns(rows);
                                }}>
                            {availableColumns
                                .filter(column => column === '' || rows.slice(0, i).indexOf(column) === -1) // Do not include already selected columns
                                .map((column, i) => {
                                    return (
                                        <option key={i} value={column}>{column}</option>
                                    )
                                })}
                        </select>
                    </div>
                );
            })}
        </>
    )
}
