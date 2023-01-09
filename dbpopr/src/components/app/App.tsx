import './App.scss';
import 'react-bootstrap-typeahead/css/Typeahead.min.css';
import 'react-bootstrap-typeahead/css/Typeahead.bs5.min.css';
import React, {useState} from 'react';
import {HashRouter, Route, Routes} from "react-router-dom";
import DatasetDetails from "../DatasetDetails";
import Datasets from "../Datasets"
import AddData from "../AddData";
import VirtualFksComponent from "../VirtualFksComponent";
import EditVirtualFkComponent from "../EditVirtualFkComponent";
import {Header} from "./Header";
import {SetupState} from "./SetupState";
import {useSetupStatusEffect} from "./useSetupStatusEffect";
import {SetupStatusComponent} from "../SetupStatusComponent";

export default function App() {
    const [setupState, setSetupState] = useState<SetupState>({activity: "Loading", error: null});

    useSetupStatusEffect(setSetupState);

    function Content() {
        if (setupState.activity) {
            return <SetupStatusComponent setupState={setupState}/>
        }else{
            return (
                <div>
                    <Routes>
                        <Route path="/dataset/:dataset" element=<DatasetDetails/>/>
                        <Route path="/add/:datasetName" element=<AddData/>/>
                        <Route path="/vfk" element=<VirtualFksComponent/>/>
                        <Route path="/vfk/add" element=<EditVirtualFkComponent/>/>
                        <Route path="/vfk/:pkTable/:fkName" element=<EditVirtualFkComponent/>/>
                        <Route path="/" element=<Datasets/>/>
                    </Routes>
                </div>
            );
        }
    }

    return (
        <>
            <HashRouter>
                <Header/>
                <div className="container">
                    <div className="text-center m-5">
                        <h1>Welcome to DbPop</h1>
                        <p className="lead">The easiest way to populate your development database.</p>
                    </div>
                    <Content/>
                </div>
            </HashRouter>
        </>
    );
}
