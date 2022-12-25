import React, {useState} from "react";
import {Navigate, NavLink, Route, Routes} from "react-router-dom";
import {usePollingEffect} from "../hooks/usePollingEffect";
import {SqlSetupStatus} from "../models/SqlSetupStatus";
import axios, {AxiosResponse} from "axios";
import DatasetsComponent from "./DatasetsComponent";
import {FilesComponent} from "./FilesComponent";
import {ApiComponent} from "./ApiComponent";
import './PopulateComponent.scss'

function PopulateComponent(): JSX.Element {
    const [error, setError] = useState<string | null>(null);
    const [loaded, setLoaded] = useState(false);

    usePollingEffect(
        async () => whenSqlSetupStatus(await axios.get<SqlSetupStatus>('/site/populate/setup')),
        {interval: 5000}
    );

    function whenSqlSetupStatus(response: AxiosResponse<SqlSetupStatus>): boolean {
        let sqlSetupStatus = response.data;
        setError(sqlSetupStatus.errorMessage);
        setLoaded(sqlSetupStatus.loaded);
        return !sqlSetupStatus.loaded;
    }

    return (
        <div id="populate-component">
            {error && (
                <div className="mb-4 alert alert-danger">
                    <div>setup.sql</div>
                    <pre className="m-3" role="alert" style={{whiteSpace: "pre-line"}}>${error}</pre>
                </div>
            )}
            {loaded || (
                <div className="mb-4 alert ">
                    <div>setup.sql:</div>
                    <div className="m-3"><i className="fa fa-fw fa-spinner fa-spin"></i> Loading...</div>
                </div>
            )}
            <ul className="nav nav-tabs" role="tablist">
                <li className="nav-item" role="presentation">
                    <NavLink to="/populate/datasets"
                             className={({isActive}) => ((isActive ? 'active' : 'inactive') + " nav-link")}
                             role="tab">
                        Datasets
                    </NavLink>
                </li>
                <li className="nav-item" role="presentation">
                    <NavLink to="/populate/files"
                             className={({isActive}) => ((isActive ? 'active' : 'inactive') + " nav-link")}
                             role="tab">
                        Files
                    </NavLink>
                </li>
                <li className="nav-item" role="presentation">
                    <NavLink to="/populate/api"
                             className={({isActive}) => ((isActive ? 'active' : 'inactive') + " nav-link")}
                             role="tab">
                        API
                    </NavLink>
                </li>
            </ul>
            <div className="tab-content">
                <Routes>
                    <Route path="datasets" element={<DatasetsComponent/>}/>
                    <Route path="files" element={<FilesComponent/>}/>
                    <Route path="api" element={<ApiComponent/>}/>
                    <Route path="/" element={<Navigate to="/populate/datasets" replace/>}/>
                </Routes>
            </div>
        </div>
    )
}

export default PopulateComponent;
