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
            <tr>
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
                            <td key={datasetContent.name}>
                                {contentElement?.rows && (
                                    <NavLink to={`/data/${datasetContent.name}/${tableName.catalog}/${tableName.schema}/${tableName.table}`}>
                                        {contentElement.rows} rows
                                    </NavLink>
                                )}
                            </td>
                        )
                    } else {
                        return (
                            <td key={datasetContent.name}>
                                {contentElement?.rows && (
                                    <>{contentElement.rows} rows</>
                                )}
                            </td>
                        )
                    }
                })}
            </tr>
        )
    }

    return (
        <div id={"datasets-component"}>
            <LoadingOverlay active={loading}/>
            <PageHeader title={"Datasets"} error={error}/>
            <div className={"table-container"}>
                {datasetContentResponse && (
                    <table id={"datasets-component"} className={"table table-hover"}>
                        <thead>
                        <tr>
                            <th style={{width: colWidth}}>Table</th>
                            {datasetContentResponse.datasetContents.map(datasetContent => <th style={{width: colWidth}} key={datasetContent.name}>{datasetContent.name}</th>)}
                        </tr>
                        </thead>
                        <tbody>
                        {datasetContentResponse.tableContents.map(tableContent => <Row key={tableNameToFqName(tableContent.tableName)} datasetContents={datasetContentResponse?.datasetContents} tableContent={tableContent}/>)}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    )
}