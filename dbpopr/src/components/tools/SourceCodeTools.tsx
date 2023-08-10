import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import download_source from "./code/sourcedownload/download_source.png"
import compare_source from "./code/sourcecompare/compare_source.png"
import {WebSocketStateContext} from "../ws/useWebSocketState";

export default function SourceCodeTools() {
    const siteStatus = useContext(WebSocketStateContext);
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"}
                        breadcrumbs={
                            siteStatus.hasTarget ? [
                                    {to: "/tools", label: "Tools"},
                                    {to: "/tools/source", label: "Source"},
                                    {label: "Code"},
                                ] :
                                [
                                    {to: "/tools/source", label: "Tools"},
                                    {label: "Code"},
                                ]
                        }
            />

            <div className={"ms-8"}>
                <Section title={"Download Source"}
                         description={"Download the tables and sprocs from the source database to the local SQL files"}
                         to={"/tools/source/download"}
                         img={download_source}
                />
                <Section title={"Compare Source"}
                         description={"Compare the tables and sprocs in the source database with the local SQL files"}
                         to={"/tools/source/compare"}
                         img={compare_source}
                />
            </div>
        </div>
    </div>
}

