import React from "react";
import PageHeader from "../pageheader/PageHeader";
import source from "./source.png"
import target from "./target.png"
import "./ToolsComponent.scss"
import {Section} from "./Section";

export default function ToolsComponent() {
    return <div id={"tools-component"} className={"container"}>
        <div>
            <PageHeader title={"Tools"} subtitle={"Download and upload code and data"}/>

            <div className={"ms-8"}>
                <Section title={"Source"}
                         description={"Tools that apply to the source database"}
                         to={"/tools/source/"}
                         img={source}/>

                <Section title={"Target"}
                         description={"Tools that apply to the target database"}
                         to={"/tools/target/"}
                         img={target}/>
            </div>
        </div>
    </div>
}