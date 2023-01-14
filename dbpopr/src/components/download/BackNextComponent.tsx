import React from "react";

export default function BackNextComponent({onCancel, onBack, onNext, onFinish}: {
    onCancel?: (() => void),
    onBack?: (() => void),
    onNext?: (() => void),
    onFinish?: (() => void),
}) {
    return <>
        <div className={"mt-3 mb-3"}>
            <div className={"btn-group"}>
                {onCancel && (
                    <button className={"btn btn-light"} onClick={onCancel}>
                        Cancel
                    </button>
                )}
                {onBack && (
                    <button className={"btn btn-primary"} onClick={onBack}>
                        <i className={"fa fa-arrow-left"}/>
                        &nbsp;
                        Back
                    </button>
                )}
                {onNext && (
                    <button className={"btn btn-primary"} onClick={onNext}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                )}
                {onFinish && (
                    <button className={"btn btn-primary"} onClick={onFinish}>
                        Finish
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                )}
            </div>
        </div>
    </>
}