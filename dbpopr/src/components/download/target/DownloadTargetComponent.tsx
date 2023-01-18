import React, {useState} from "react";
import LoadingOverlay from "../../utils/LoadingOverlay";
import PageHeader from "../../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import {dowloadTarget} from "../../../api/dowloadTarget";
import {DownloadResponse} from "../../../models/DownloadResponse";
import {tableNameToFqName} from "../../../models/TableName";
import {Plural} from "../../../utils/DbPopUtils";
import useDatasets from "../../utils/useDatasets";

export default function DownloadTargetComponent() {
    const [loading, setLoading] = useState(false);
    const [datasets, loadingDatasets] = useDatasets();
    const [dataset, setDataset] = useState('base');
    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse>();

    function onDownload() {
        setLoading(true);
        dowloadTarget(dataset)
            .then(result => {
                setDownloadResponse(result.data);
            })
            .finally(() => {
                setLoading(false);
            })
    }

    function InputComponent() {
        return <>
            <LoadingOverlay active={loading || loadingDatasets}/>
            <PageHeader title={"Full Download"} subtitle={"Dowload the content of the target database to CSV"}/>
            <div className={"mt-3 mb-3 button-bar"}>
                <div className={"btn-group"}>
                    <NavLink to={"/download/"} className={"btn btn-primary"}>
                        <i className={"fa fa-arrow-left"}/>
                        &nbsp;
                        Back
                    </NavLink>
                    <button className={"btn btn-primary"} onClick={onDownload}>
                        Download
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </div>
            </div>

            <div className={"row"}>
                <div className={"col-3"}>
                    <label htmlFor="dataset" className="form-label">Dataset:</label>
                    <select className="form-select" id="datasets" defaultValue={dataset} onChange={e => setDataset(e.target.value)}>
                        {datasets
                            .filter(it => it !== 'static')
                            .map(ds => (
                                <option key={ds} value={ds}>{ds}</option>
                            ))}
                    </select>
                </div>
            </div>
        </>
    }

    function ResultComponent({downloadResponse}: { downloadResponse: DownloadResponse }) {
        return <>
            <PageHeader title={"Full Download"} subtitle={`The CSV files have been added to the ${dataset} dataset`}/>
            <div className={"mt-3 mb-3 button-bar"}>
                <div className={"btn-group"}>
                    <NavLink to={"/"} className={"btn btn-primary"}>
                        Close
                    </NavLink>
                </div>
            </div>
            <div className={"table-container"}>
                <table className={"table table-hover"}>
                    <thead>
                    <tr>
                        <th>Table</th>
                        <th className={"text-end"}>Rows</th>
                        <th className={"text-end"}>Skipped</th>
                    </tr>
                    </thead>
                    <tbody>
                    {downloadResponse.tableRowCounts.map(tableRowCount => (
                        <tr key={tableNameToFqName(tableRowCount.tableName)}>
                            <td>{tableNameToFqName(tableRowCount.tableName)}</td>
                            <td className={"text-end"}>
                                {tableRowCount.rowCount ? Plural(tableRowCount.rowCount, "row") : '-'}
                            </td>
                            <td className={"text-end"}>
                                {tableRowCount.rowsSkipped ? Plural(tableRowCount.rowsSkipped, "row") : '-'}
                            </td>
                        </tr>
                    ))}
                    </tbody>
                    <tfoot>
                    <tr>
                        <th>Total</th>
                        <th className={"text-end"}>
                            {downloadResponse.rowCount ? Plural(downloadResponse.rowCount, "row") : '-'}
                        </th>
                        <th className={"text-end"}>
                            {downloadResponse.rowsSkipped ? Plural(downloadResponse.rowsSkipped, "row") : '-'}
                        </th>
                    </tr>
                    </tfoot>
                </table>
            </div>
        </>
    }

    return <div id={"download-target"}>
        {!downloadResponse && <InputComponent/>}
        {downloadResponse && <ResultComponent downloadResponse={downloadResponse}/>}
    </div>
}