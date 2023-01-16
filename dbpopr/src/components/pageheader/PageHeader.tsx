import React from "react";
import './PageHeader.scss'

export default function PageHeader({title, subtitle, error}: {
    title: string,
    subtitle?: string,
    error?: string,
}) {
    return <>
        <div className={"pageheader-component"}>
            <header>{title}</header>
            <p><small>{subtitle}</small></p>
        </div>
        {error && (
            <div className={"alert alert-danger"}>{error}</div>
        )}
    </>
}