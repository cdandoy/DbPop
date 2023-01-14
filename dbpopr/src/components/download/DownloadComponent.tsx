import React from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";

export const Pages = {
    "selectMode": "selectMode",
    "selectBulkTables": "selectBulkTables",
    "selectBulkDependencies": "selectBulkDependencies",
}

export default function DownloadComponent() {

    return <>
        <PageHeader title={"Download"} subtitle={"Download table data to CSV files"}/>
        <div>
            <h3>
                Download a Model
            </h3>
            <p>Select multiple tables and the dependencies established based database constraints, and filter the data you want to download.</p>
            <div className={"ms-5"}>
                <NavLink to={"/download/model"}>
                    <button className={"btn btn-primary"}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </NavLink>
            </div>
        </div>
        <div className={"mt-5"}>
            <h3>
                Bulk Download
            </h3>
            <p>Select multiple tables and download the full content.</p>
            <div className={"ms-5"}>
                <NavLink to={"/download/bulk"}>
                    <button className={"btn btn-primary"}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </NavLink>
            </div>
        </div>
    </>;
}