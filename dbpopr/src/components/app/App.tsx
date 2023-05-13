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
import useWebSocket from 'react-use-websocket';
import {Change, DefaultChanges, targetChanges} from "../../api/changeApi";

export const SiteContext = React.createContext<SiteResponse>(DefaultSiteResponse);
export const ChangeContext = React.createContext<Change[]>([]);

const WS_URL = 'ws://localhost:8080/ws/site';

interface Message {
    messageType: string
}

export default function App() {
    const [siteStatus, setSiteStatus] = useState<SiteStatus>({statuses: [], complete: false});
    const [siteResponse, setSiteResponse] = useState<SiteResponse>(DefaultSiteResponse);
    const [changes, setChanges] = useState<Change[]>(DefaultChanges);
    const { lastJsonMessage } = useWebSocket(WS_URL);

    useEffect(() => {
        siteApi()
            .then(result => setSiteResponse(result.data))
    }, [siteStatus]);

    useSetupStatusEffect(setSiteStatus);

    useEffect(() => {
        targetChanges()
            .then(result => {
                setChanges(result.data);
            })
    }, []);

    useEffect(() => {
        if (lastJsonMessage) {
            const message = lastJsonMessage as any as Message;
            if (message.messageType === 'CODE_CHANGE') {
                targetChanges()
                    .then(result => {
                        setChanges(result.data);
                    })
            }
        }
    }, [lastJsonMessage]);


    if (siteStatus.complete) {
        return (
            <>
                <HashRouter>
                    <SiteContext.Provider value={siteResponse}>
                        <ChangeContext.Provider value={changes}>
                            <Sidebar title1={"DbPop"} title2={`v${process.env.REACT_APP_VERSION}`} menu=<Menu/> bottomMenu=<BottomMenu/> >
                                <Header/>
                                <div className="container">
                                    <RoutesComponent/>
                                </div>
                            </Sidebar>
                        </ChangeContext.Provider>
                    </SiteContext.Provider>
                </HashRouter>
            </>
        );
    } else {
        return <SiteStatusComponent siteStatus={siteStatus}/>
    }
}
