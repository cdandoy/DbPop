import React from "react";
import './PageHeader.scss'
import {Breadcrumb, Breadcrumbs} from "./Breadcrumbs";

export default function PageHeader({title, subtitle, error, tool, breadcrumbs}: {
    title: string;
    subtitle?: string;
    error?: string;
    tool?: JSX.Element;
    breadcrumbs?: Breadcrumb[];
}) {
    return <>
        <div className={"pageheader-component row"}>
            <div className={"col-8"}>
                <div className={"mb-3"}>
                    <header>{title}</header>
                    <div><small>{subtitle}</small></div>
                    <Breadcrumbs breadcrumbs={breadcrumbs}/>
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