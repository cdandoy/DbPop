import React, {useState} from "react";
import {NavLink, useParams} from "react-router-dom";
import {SearchTableResult, SelectTable} from "./SelectTable";
import {DependentTables} from "./DependentTables";
import FilterForm from "./FilterForm";
import RowCounts from "./RowCounts";
import {executeDownload} from "./executeDownload";
import {Dependency} from "../models/Dependency";
import {DownloadResponse} from "../models/DownloadResponse";

export default function AddData() {
    const routeParams = useParams();
    const datasetName = routeParams['datasetName']
    const [tableSelection, setTableSelection] = useState<SearchTableResult | null>(null);
    const [dependency, setDependency] = useState<Dependency | null>(null);
    const [queryValues, setQueryValues] = useState<any>({})
    const [dependencyChangeNumber, setDependencyChangeNumber] = useState<number>(0);
    const [searchChangeNumber, setSearchChangeNumber] = useState<number>(0);
    const [rowCounts, setRowCounts] = useState<DownloadResponse | null>(null);
    const [downloadedMessage, setDownloadedMessage] = useState<string | null>(null);

    function whenSearchSubmitted() {
        setDownloadedMessage(null);
        setSearchChangeNumber(searchChangeNumber + 1);
    }

    function whenDownload() {
        setDownloadedMessage(null);
        if (datasetName != null && dependency != null) {
            executeDownload(datasetName, dependency, queryValues, false)
                .then(response => {
                    if (response.data.rowCount > 0) {
                        setRowCounts(null);
                        setDownloadedMessage(`Downloaded ${response.data.rowCount} rows`);
                    } else {
                        setDownloadedMessage(`Nothing to download`);
                    }
                })
        }
    }

    if (!datasetName) {
        return <div>Missing Dataset</div>
    }

    return (
        <div>
            {/*Breadcrumbs*/}
            <nav aria-label="breadcrumb">
                <ol className="breadcrumb">
                    <li className="breadcrumb-item"><NavLink to="/">Home</NavLink></li>
                    <li className="breadcrumb-item active" aria-current="page">Add to: <strong>{datasetName}</strong></li>
                </ol>
            </nav>

            {/* Table Selection drop-down*/}
            <div className="mb-3">
                <label htmlFor="table-name" className="form-label">Table Name:</label>
                <SelectTable setTableSelection={(searchTableResult: SearchTableResult | null) => {
                    setTableSelection(searchTableResult);
                    if (searchTableResult) {
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
                }}
                />
            </div>

            {/*Dependent Tables*/}
            <div className="mb-3">
                <label htmlFor="table-name" className="form-label">Dependent:</label>
                <DependentTables rootTable={tableSelection}
                                 changeNumber={dependencyChangeNumber}
                                 setChangeNumber={setDependencyChangeNumber}
                                 dependency={dependency}
                                 setDependency={setDependency}
                />
            </div>

            {/*Filter*/}
            <FilterForm rootTable={tableSelection}
                        queryValues={queryValues}
                        setQueryValues={setQueryValues}
                        whenSearchSubmitted={whenSearchSubmitted}
            />

            {/*Row Counts*/}
            {dependency && queryValues && searchChangeNumber > 0 &&
                <div className={"mt-4"}>
                    <RowCounts
                        rowCounts={rowCounts}
                        setRowCounts={setRowCounts}
                        dataset={datasetName}
                        changeNumber={searchChangeNumber}
                        dependency={dependency}
                        queryValues={queryValues}
                    />
                </div>
            }

            {/*Download*/}
            {rowCounts && rowCounts.rowCount > 0 &&
                <div className={"mt-4"}>
                    <button className={"btn btn-primary"} onClick={whenDownload}>
                        <i className={"fa fa-download"}/> Download
                    </button>
                </div>
            }

            {downloadedMessage &&
                <div>{downloadedMessage}</div>
            }
        </div>
    );
}