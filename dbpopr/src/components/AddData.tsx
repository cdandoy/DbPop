import React, {useState} from "react";
import {useParams} from "react-router-dom";
import {SelectTable} from "./SelectTable";
import {DependentTables} from "./DependentTables";
import FilterForm from "./FilterForm";
import RowCounts from "./RowCounts";
import {executeDownload} from "./executeDownload";
import {Dependency} from "../models/Dependency";
import {DownloadResponse} from "../models/DownloadResponse";
import {Table} from "../models/Table";
import PageHeader from "./pageheader/PageHeader";

export default function AddData() {
    const routeParams = useParams();
    const datasetName = routeParams['datasetName']
    const [table, setTable] = useState<Table | null>(null);
    const [dependency, setDependency] = useState<Dependency | null>(null);
    const [queryValues, setQueryValues] = useState<any>({})
    const [dependencyChangeNumber, setDependencyChangeNumber] = useState<number>(0);
    const [searchChangeNumber, setSearchChangeNumber] = useState<number>(0);
    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse | null>(null);
    const [downloadedMessage, setDownloadedMessage] = useState<string | null>(null);
    const [dowloading, setDowloading] = useState<boolean>(false);
    const [maxRows, setMaxRows] = useState(1000);

    function resetMaxRows() {
        setMaxRows(1000);
    }

    function whenSearchSubmitted() {
        setDownloadedMessage(null);
        setSearchChangeNumber(searchChangeNumber + 1);
        resetMaxRows();
    }

    function whenDownload() {
        setDowloading(true);
        setDownloadedMessage(null);
        setSearchChangeNumber(0);
        if (datasetName != null && dependency != null) {
            executeDownload(datasetName, dependency, queryValues, false, 0)
                .then(response => {
                    setDowloading(false);
                    setDownloadResponse(null);
                    if (response.data.rowCount > 0) {
                        setDownloadResponse(null);
                        setDownloadedMessage(`Downloaded ${response.data.rowCount} rows`);
                    } else {
                        setDownloadedMessage(`Nothing to download`);
                    }
                })
                .catch((result) => {
                    setDowloading(false);
                    setDownloadedMessage(result);
                })
        }
    }

    if (!datasetName) {
        return <div>Missing Dataset</div>
    }

    if (dowloading) {
        return <div className="m-3"><i className="fa fa-fw fa-spinner fa-spin"></i> Downloading...</div>;
    }

    return (
        <div>
            <PageHeader title={`Download`} subtitle={`Download Data to "${datasetName}"`}/>

            {/* Table Selection drop-down*/}
            <div className="mb-3">
                <label htmlFor="table-name" className="form-label">Table Name:</label>
                <SelectTable setTable={(table: Table) => {
                    setTable(table);
                    const root: Dependency = {
                        displayName: 'root',
                        tableName: table.tableName,
                        constraintName: null,
                        subDependencies: null,
                        selected: true,
                        mandatory: true
                    };
                    setDependency(root);
                }}/>
            </div>

            {/*Dependent Tables*/}
            {table && (
                <div className="mb-3">
                    <label htmlFor="table-name" className="form-label">Dependent:</label>
                    <DependentTables dependency={dependency}
                                     setDependency={setDependency}
                                     changeNumber={dependencyChangeNumber}
                                     setChanged={() => {
                                         setDependencyChangeNumber(dependencyChangeNumber + 1);
                                         resetMaxRows();
                                     }}
                    />
                </div>
            )}

            {/*Filter*/}
            {table && (
                <FilterForm rootTable={table}
                            queryValues={queryValues}
                            setQueryValues={setQueryValues}
                            whenSearchSubmitted={whenSearchSubmitted}
                />
            )}

            {/*Row Counts*/}
            {dependency && queryValues && searchChangeNumber > 0 &&
                <div className={"mt-4"}>
                    <RowCounts
                        downloadResponse={downloadResponse}
                        setDownloadResponse={setDownloadResponse}
                        dataset={datasetName}
                        changeNumber={searchChangeNumber}
                        dependency={dependency}
                        queryValues={queryValues}
                        maxRows={maxRows}
                        setMaxRows={setMaxRows}
                    />
                </div>
            }

            {/*Download*/}
            {downloadResponse && downloadResponse.rowCount > 0 && !downloadResponse.maxRowsReached &&
                <div className={"mt-4"}>
                    <button className={"btn btn-primary"} onClick={whenDownload}>
                        <i className={"fa fa-download"}/> Download
                    </button>
                </div>
            }

            {downloadedMessage &&
                <div className={"mt-4 alert alert-success"}>{downloadedMessage}</div>
            }
        </div>
    );
}