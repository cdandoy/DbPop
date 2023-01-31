import React from "react";
import './PageHeader.scss'

export default function PageHeader({title, subtitle, error, tool}: {
    title: string;
    subtitle?: string;
    error?: string;
    tool?: JSX.Element;
}) {
    return <>
        <div className={"pageheader-component row"}>
            <div className={"col-8"}>
                <div>
                    <header>{title}</header>
                    <p><small>{subtitle}</small></p>
                </div>
            </div>
            <div className={"col-4 text-end"}>
                {tool}
            </div>
        </div>
        {error && (
            <div className={"alert alert-danger"}>{error}</div>
        )}
    </>
}