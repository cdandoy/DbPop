import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React, {useEffect, useState} from 'react';
import {HashRouter} from "react-router-dom";
import {Header} from "./Header";
import Menu from "./Menu";
import BottomMenu from "./BottomMenu";
import {SetupState} from "./SetupState";
import {useSetupStatusEffect} from "./useSetupStatusEffect";
import {SetupStatusComponent} from "../SetupStatusComponent";
import Sidebar from "../sidebar/Sidebar";
import RoutesComponent from "../RoutesComponent";
import {DefaultSiteResponse, siteApi, SiteResponse} from "../../api/siteApi";

export const SiteContext = React.createContext<SiteResponse>(DefaultSiteResponse);

export default function App() {
    const [setupState, setSetupState] = useState<SetupState>({activity: "Loading", error: null});
    const [siteResponse, setSiteResponse] = useState<SiteResponse>(DefaultSiteResponse);

    useEffect(() => {
        siteApi()
            .then(result => {
                setSiteResponse(result.data);
            })
    }, []);

    useSetupStatusEffect(setSetupState);

    function Debug() {
        const debug = false;

        function flipProperty(name: string) {
            const newVal = {...siteResponse};
            (newVal as any)[name] = !((newVal as any)[name]);
            setSiteResponse({...siteResponse, ...newVal})
        }

        if (!debug) return <></>;
        return (
            <div>
                {["hasSource", "hasTarget"].map(name => (
                    <span key={name}
                          style={{marginLeft: "2em"}}
                          onClick={() => flipProperty(name)}>{name}: {(siteResponse as any)[name] ? 'Yes' : 'No'}
                    </span>
                ))}
            </div>
        )
    }

    function Content() {
        if (setupState.activity) {
            return <SetupStatusComponent setupState={setupState}/>
        } else {
            return (
                <div>
                    <RoutesComponent/>
                </div>
            );
        }
    }

    return (
        <>
            <HashRouter>
                <SiteContext.Provider value={siteResponse}>
                    <Sidebar title1={"DbPop"} title2={`v${process.env.REACT_APP_VERSION}`} menu=<Menu/> bottomMenu=<BottomMenu/> >
                        <Debug/>
                        <Header/>
                        <div className="container">
                            <Content/>
                        </div>
                    </Sidebar>
                </SiteContext.Provider>
            </HashRouter>
        </>
    );
}
