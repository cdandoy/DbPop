import React, {useEffect, useState} from "react";
import PageHeader from "../pageheader/PageHeader";
import {NavLink} from "react-router-dom";
import {siteApi, SiteResponse} from "../../api/siteApi";

function Section({title, description, to}: {
    title: string;
    description: string | JSX.Element;
    to: string;
}) {
    return <>
        <div className={"mt-5"}>
            <h3>{title}</h3>
            <p>{description}</p>
            <div className={"ms-5 button-bar"}>
                <NavLink to={to}>
                    <button className={"btn btn-primary"}>
                        Next
                        &nbsp;
                        <i className={"fa fa-arrow-right"}/>
                    </button>
                </NavLink>
            </div>
        </div>
    </>
}

export default function CodeComponent() {
    const [siteResponse, setSiteResponse] = useState<SiteResponse | undefined>();

    useEffect(() => {   // I should use context or redux for that
        siteApi()
            .then(result => {
                setSiteResponse(result.data);
            })
    }, []);

    return <>
        <div>
            <PageHeader title={"Code"} subtitle={"Tables and Stored Procedures"}/>

            {siteResponse?.hasSource && (
                <Section title={"Compare Source"}
                         description={"Compare the source database with the local files"}
                         to={"/code/source/compare"}/>
            )}

            {siteResponse?.hasSource && (
                <Section title={"Download Source"}
                         description={"Download the source database to the local filesystem"}
                         to={"/code/source/download"}/>
            )}

            {siteResponse?.hasTarget && (
                <Section title={"Upload to Target"}
                         description={"Create"}
                         to={"/code/target/upload"}/>
            )}

        </div>
    </>
}