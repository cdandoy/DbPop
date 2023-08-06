import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import target_full from "./data/target_full/target_full.png"
import {SiteStatusContext} from "../app/App";

export default function TargetDataTools() {
    const siteStatus = useContext(SiteStatusContext);
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"}
                        breadcrumbs={
                            siteStatus.hasSource ? [
                                    {to: "/tools", label: "Tools"},
                                    {to: "/tools/target", label: "Target"},
                                    {label: "Data"},
                                ] :
                                [
                                    {to: "/tools/target", label: "Tools"},
                                    {label: "Data"},
                                ]
                        }
            />

            <div className={"ms-8"}>
                <Section title={"Full Target Download"}
                         description={<>Dump the full content of your TARGET database back to CSV files.<br/>
                             This might be useful if you use your application to generate additional data and want to save the result to CSV files.</>}
                         to={"/tools/target/full"}
                         img={target_full}
                />
            </div>
        </div>
    </div>
}

