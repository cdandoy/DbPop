import React, {useEffect, useState} from 'react';
import './Datasets.scss'
import {NavLink} from "react-router-dom";
import PageHeader from "../pageheader/PageHeader";
import {tableNameToFqName} from "../../models/TableName";
import {DatasetContent, datasetContent, DatasetContentResponse, TableContent} from "../../api/datasetContent";
import LoadingOverlay from "../utils/LoadingOverlay";

export default function Datasets() {
    const [loading, setLoading] = useState(false);
    const [datasetContentResponse, setDatasetContentResponse] = useState<DatasetContentResponse | null>(null);
    const [error, setError] = useState<string | undefined>();
    const [colWidth, setColWidth] = useState("0px");
    const showLinks = false;

    useEffect(() => {
        setLoading(true);
        datasetContent()
            .then(result => {
                setDatasetContentResponse(result.data);
                const nbrCols = result.data.datasetContents.length + 1;
                setColWidth((100 / nbrCols) + "%");
            })
            .catch(error => {
                setError(error.message || 'internal error');
            })
            .finally(() => {
                setLoading(false);
            })
    }, [])

    function Row({datasetContents, tableContent}: {
        datasetContents: DatasetContent[],
        tableContent: TableContent,
    }) {
        const tableName = tableContent.tableName;
        return (
            <tr key={tableNameToFqName(tableName)}>
                <td key={"table.name"}>
                    {showLinks && (
                        <NavLink to={`/data/${tableName.catalog}/${tableName.schema}/${tableName.table}`}>
                            {tableNameToFqName(tableName)}
                        </NavLink>
                    )}
                    {!showLinks && tableNameToFqName(tableName)}
                </td>
                {datasetContents.map(datasetContent => {
                    const contentElement = tableContent.content[datasetContent.name];
                    if (showLinks) {
                        return (
                            <td key={datasetContent.name} className={"text-end"}>
                                {contentElement?.rows && (
                                    <NavLink to={`/data/${datasetContent.name}/${tableName.catalog}/${tableName.schema}/${tableName.table}`}>
                                        {contentElement.rows.toLocaleString()} rows
                                    </NavLink>
                                )}
                            </td>
                        )
                    } else {
                        return (
                            <td key={datasetContent.name} className={"text-end"}>
                                {contentElement?.rows && (
                                    <>{contentElement.rows.toLocaleString()}</>
                                )}
                            </td>
                        )
                    }
                })}
            </tr>
        )
    }

    return (
        <div id={"datasets-component"} className={"container"}>
            <LoadingOverlay active={loading}/>
            <PageHeader title={"Datasets"} error={error}/>
            {datasetContentResponse && (
                <>
                    <h1 className={"row pageheader-component pb-5"}>
                        <div key={"head-1"} className={"col-6 text-center"}>
                            {datasetContentResponse.datasetContents
                                .map(value => value.fileCount)
                                .reduce((p, c) => p + c, 0)
                                .toLocaleString()
                            } tables
                        </div>
                        <div key={"head-2"} className={"col-6 text-center"}>
                            {datasetContentResponse.datasetContents
                                .map(value => value.rows)
                                .reduce((p, c) => p + c, 0)
                                .toLocaleString()
                            } rows
                        </div>
                    </h1>
                    <div className={"table-container"}>
                        <table id={"datasets-component"} className={"table table-hover"}>
                            <thead>
                            <tr key={"header"}>
                                <th style={{width: colWidth}}>Table</th>
                                {datasetContentResponse.datasetContents.map(datasetContent => (
                                    <th style={{width: colWidth}} key={datasetContent.name} className={"text-end"}>
                                        {datasetContent.name}
                                    </th>
                                ))}
                            </tr>
                            </thead>
                            <tbody>
                            {datasetContentResponse.tableContents.map(tableContent =>
                                <Row key={tableNameToFqName(tableContent.tableName)} datasetContents={datasetContentResponse?.datasetContents} tableContent={tableContent}/>
                            )}
                            </tbody>
                            <tfoot>
                            <tr key={"footer-1"}>
                                <th>Total</th>
                                {datasetContentResponse.datasetContents.map(datasetContent => (
                                    <th key={datasetContent.name} className={"text-end"}>
                                        {datasetContent.rows.toLocaleString()} rows
                                    </th>
                                ))}
                            </tr>
                            <tr key={"footer-2"}>
                                <th></th>
                                {datasetContentResponse.datasetContents.map(datasetContent => (
                                    <th key={datasetContent.name} className={"text-end"}>
                                        {datasetContent.fileCount.toLocaleString()} tables
                                    </th>
                                ))}
                            </tr>
                            </tfoot>
                        </table>
                    </div>
                </>
            )}
        </div>
    );
}