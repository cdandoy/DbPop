import React, {useContext} from "react";
import PageHeader from "../pageheader/PageHeader";
import {Section} from "./Section";
import upload_target from "./code/targetupload/upload_target.png"
import download_target from "./code/targetdownload/download_target.png"
import compare_target from "./code/targetcompare/compare_target.png"
import {WebSocketStateContext} from "../ws/useWebSocketState";

export default function TargetCodeTools() {
    const siteStatus = useContext(WebSocketStateContext);
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"}
                        breadcrumbs={
                            siteStatus.hasSource ? [
                                    {to: "/tools", label: "Tools"},
                                    {to: "/tools/target", label: "Target"},
                                    {label: "Code"},
                                ] :
                                [
                                    {to: "/tools/target", label: "Tools"},
                                    {label: "Code"},
                                ]
                        }
            />

            <div className={"ms-8"}>
                <Section title={"Compare Target"}
                         description={"Compare the tables and sprocs in the Target database with the local SQL files"}
                         to={"/tools/target/compare"}
                         img={compare_target}
                />
                <Section title={"Upload to Target"}
                         description={"Upload the tables and sprocs from the SQL files to the Target database"}
                         to={"/tools/target/upload"}
                         img={upload_target}
                />
                <Section title={"Download from Target"}
                         description={"Download the tables and sprocs from the Target database to the SQL files"}
                         to={"/tools/target/download"}
                         img={download_target}
                />
            </div>
        </div>
    </div>
}

