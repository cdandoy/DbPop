import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React, {useEffect, useState} from 'react';
import {HashRouter} from "react-router-dom";
import {Header} from "./Header";
import Menu from "./Menu";
import BottomMenu from "./BottomMenu";
import Sidebar from "../sidebar/Sidebar";
import RoutesComponent from "../RoutesComponent";
import {DefaultSiteResponse, siteApi, SiteResponse} from "../../api/siteApi";
import {SiteStatus, useSetupStatusEffect} from "./sitestatus/useSetupStatusEffect";
import SiteStatusComponent from "./sitestatus/SiteStatusComponent";
import {useWebSocketState, WebSocketStateContext} from "../ws/useWebSocketState";
import {ServerDisconnected} from "./ServerDisconnected";

export const SiteContext = React.createContext<SiteResponse>(DefaultSiteResponse);

export default function App() {
    const [siteStatus, setSiteStatus] = useState<SiteStatus>({statuses: [], complete: false});
    const [siteResponse, setSiteResponse] = useState<SiteResponse>(DefaultSiteResponse);
    const messageState = useWebSocketState();

    useEffect(() => {
        siteApi()
            .then(result => setSiteResponse(result.data))
    }, [siteStatus]);

    useSetupStatusEffect(setSiteStatus);

    if (siteStatus.complete) {
        return (
            <>
                <HashRouter>
                    <SiteContext.Provider value={siteResponse}>
                        <WebSocketStateContext.Provider value={messageState}>
                            <Sidebar title1={"DbPop"} title2={`v${process.env.REACT_APP_VERSION}`} menu=<Menu/> bottomMenu=<BottomMenu/> >
                                <Header/>
                                <div className="container">
                                    <RoutesComponent/>
                                </div>
                            </Sidebar>
                            {messageState.connected || <ServerDisconnected/>}
                        </WebSocketStateContext.Provider>
                    </SiteContext.Provider>
                </HashRouter>
            </>
        );
    } else {
        return <SiteStatusComponent siteStatus={siteStatus}/>
    }
}
