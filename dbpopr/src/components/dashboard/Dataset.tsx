import {Plural, toHumanReadableSize} from "../../utils/DbPopUtils";
import React from "react";
import {DatasetContent} from "../../api/datasetContent";

export function Dataset({
                            datasetContent,
                            hasUpload,
                            loadingDataset,
                            loadDataset,
                        }: {
    datasetContent: DatasetContent;
    hasUpload: boolean;
    loadingDataset: string | null,
    loadDataset: (p: string) => void,
}) {
    const datasetName = datasetContent.name;

    function RefreshButton({disabled, title, hidden, loading, onClick}: {
        title: string;
        disabled?: boolean;
        hidden?: boolean;
        loading?: boolean;
        onClick?: () => void;
    }) {
        let iconClass = loading ? "fa fa-fw fa-spinner fa-spin" : "fa fa-fw fa-refresh";
        return (
            <button className="btn btn-xs btn-primary"
                    title={title}
                    disabled={disabled}
                    style={{display: hidden ? 'none' : "initial"}}
                    onClick={() => {
                        if (!disabled && !hidden && onClick) {
                            onClick();
                        }
                    }}>
                <i className={iconClass}/>
            </button>
        );
    }

    function buttonContent() {
        const title = !hasUpload ? "Target database not available" :
            !!loadingDataset ? "Loading" :
                "Reload";

        return <RefreshButton title={title}
                              hidden={datasetName === 'static'}
                              disabled={!!loadingDataset}
                              loading={loadingDataset === datasetName}
                              onClick={() => loadDataset(datasetName)}/>
    }

    function statusContent() {
        if (datasetContent.failureCauses) {
            return (
                <div className="mb-2 alert alert-danger">
                        <pre className="dataset-error">
                            {datasetContent.failureCauses.join('\n')}
                        </pre>
                </div>
            )
        } else if (loadingDataset === datasetName) {
            return (
                <div className="mb-2">
                    <div className='dataset-result'>
                        Loading
                    </div>
                </div>
            )
        } else if (datasetContent.active) {
            return (
                <div className="mb-2">
                    <div className='dataset-result'>
                        Loaded {Plural(datasetContent.rows, 'row')} in {datasetContent.executionTime}ms
                    </div>
                </div>
            )
        } else {
            return (
                <div className="mb-2 text-muted">
                    {Plural(datasetContent.fileCount, 'table')}
                    <>,&nbsp;</>
                    {Plural(datasetContent.rows, 'row')}
                    <>,&nbsp;</>
                    {toHumanReadableSize(datasetContent.size).text}
                </div>
            );
        }
    }

    const cardStyle = datasetContent.active ? {borderColor: "limegreen", borderWidth: "2px"} : {};
    return (
        <div className="card ds-card" style={cardStyle}>
            <div className="ds-body">
                <div className={"ds-button"}>
                    {buttonContent()}
                </div>
                <div className={"ds-content"}>
                    <div className={"ds-name"}>
                        <strong>{datasetName}</strong>
                    </div>
                    <div className={"ds-info"}>
                        {statusContent()}
                    </div>
                </div>
            </div>
        </div>
    )
}