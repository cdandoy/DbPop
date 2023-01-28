import SidebarMenu from "../sidebar/SidebarMenu";
import React from "react";

export default function BottomMenu() {
    return <>
        <li>
            <a href="/swagger-ui/" target="_blank">
                <SidebarMenu text="API" icons="fa fa-book"/>
            </a>
        </li>
        <li>
            <a href="https://github.com/cdandoy/DbPop" target="_blank" rel="noreferrer">
                <SidebarMenu text="GitHub" icons="fa-brands fa-docker"/>
            </a>
        </li>
        <li>
            <a href="https://hub.docker.com/r/cdandoy/dbpop" target="_blank" rel="noreferrer">
                <SidebarMenu text="Docker" icons="fa-brands fa-github"/>
            </a>
        </li>
    </>
}