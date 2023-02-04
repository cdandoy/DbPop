import React, {useEffect, useState} from "react";
import SelectTableComponent from "./SelectTableComponent";
import {content, TableInfo} from "../../../api/content";
import LoadingOverlay from "../../utils/LoadingOverlay";
import DependenciesComponent, {DependenciesFilter} from "./DependenciesComponent";
import {Dependency} from "../../../models/Dependency";
import './StructuredDownloadComponent.scss'
import {TableName, tableNameEquals, tableNameToFqName} from "../../../models/TableName";
import DataFilterComponent, {DependencyQuery} from "./DataFilterComponent";
import DownloadResultsComponent from "./DownloadResultsComponent";
import {DownloadResponse} from "../../../models/DownloadResponse";
import dependenciesApi from "../../../api/dependenciesApi";
import {executeDownload} from "../../../api/executeDownload";
import useDatasets from "../../utils/useDatasets";

export default function StructuredDownloadComponent() {
    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse | undefined>();
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    const [page, setPage] = useState("baseTable");
    // Loading
    const [loadingContent, setLoadingContent] = useState(false);
    const [loadingDependencies, setLoadingDependencies] = useState(false);
    const [loadingDataFilter, setLoadingDataFilter] = useState(false);
    const [loadingCsv, setLoadingCsv] = useState(false);

    const [error, setError] = useState<string | undefined>();

    // SelectTableComponent
    const [tableInfos, setTableInfos] = useState<TableInfo[]>([]);
    const [nameFilter, setNameFilter] = useState("");
    const [nameRegExp, setNameRegExp] = useState<RegExp>(/.*/);
    const [tableDependenciesFilter, setTableDependenciesFilter] = useState(true);
    const [rootTableName, setRootTableName] = useState<TableName | null>(null);

    // DependenciesComponent
    const [dependency, setDependency] = useState<Dependency | null>(null);
    const [selectedDependency, setSelectedDependency] = useState<Dependency | null>(null);
    const [dependenciesFilter, setDependenciesFilter] = useState<DependenciesFilter>({
        required: false,
        recommended: true,
        optional: false
    });

    // DataFilterComponent
    const [autoRefresh, setAutoRefresh] = useState(false);
    const [dirty, setDirty] = useState(true);
    const [dataFilterChangeNumber, setDataFilterChangeNumber] = useState(0);
    const [datasets, loadingDatasets] = useDatasets();
    const [dataset, setDataset] = useState('base');
    const [rowLimit, setRowLimit] = useState(1000);
    const [dependencyQueries, setDependencyQueries] = useState<DependencyQuery[]>([]);
    const [previewResponse, setPreviewResponse] = useState<DownloadResponse | undefined>();

    // SelectTableComponent Input
    useEffect(() => {
        setLoadingContent(true);
        content()
            .then(result => {
                setTableInfos(result.data);
                setLoadingContent(false);
            })
    }, []);

    // DependenciesComponent Input
    useEffect(() => {
        if (page === "dependencies" && rootTableName) {
            setLoadingDependencies(true)
            const rootDependency: Dependency = {
                tableName: rootTableName,
                selected: true,
                constraintName: null,
                subDependencies: [],
                displayName: tableNameToFqName(rootTableName),
                mandatory: true
            }
            dependenciesApi(rootDependency)
                .then(result => {
                    setDependency(result.data);
                    setLoadingDependencies(false);
                });
        }
    }, [rootTableName, page]);

    useEffect(() => {
        if (selectedDependency != null) {
            setLoadingDependencies(true)
            dependenciesApi(selectedDependency)
                .then(result => {
                    setDependency(result.data);
                })
                .finally(() => {
                    setLoadingDependencies(false);
                });
        }
    }, [selectedDependency]);

    function applyQueries(dependency: Dependency, dependencyQueries: DependencyQuery[]) {
        for (const dependencyQuery of dependencyQueries) {
            if (dependencyQuery.constraintName === dependency.constraintName && tableNameEquals(dependencyQuery.tableName, dependency.tableName)) {
                dependency.queries = dependencyQuery.queries;
            }
        }
        if (dependency.subDependencies) {
            for (const subDependency of dependency.subDependencies) {
                applyQueries(subDependency, dependencyQueries);
            }
        }
    }

    useEffect(() => {
        if (page === 'dataFilter' && selectedDependency && (autoRefresh || dataFilterChangeNumber > 0)) {
            setLoadingDataFilter(true);
            // Take a copy of selectedDependency and copy the queries over it
            const dependency = structuredClone(selectedDependency);
            applyQueries(dependency, dependencyQueries);
            executeDownload(dataset, dependency, {}, true, rowLimit)
                .then(result => {
                    setPreviewResponse(result.data);
                    setDirty(false);
                })
                .finally(() => {
                    setLoadingDataFilter(false);
                })
        }
    }, [page, dataset, selectedDependency, rowLimit, dataFilterChangeNumber])

    function refreshDataFilter() {
        setDataFilterChangeNumber(dataFilterChangeNumber + 1);
    }

    function onDownload() {
        // Take a copy of selectedDependency and copy the queries over it
        if (selectedDependency) {
            setLoadingCsv(true);
            setError(undefined);
            const dependency = structuredClone(selectedDependency);
            applyQueries(dependency, dependencyQueries);
            executeDownload(dataset, dependency, {}, false, rowLimit)
                .then(result => {
                    setDownloadResponse(result.data);
                    setPage("download-result");
                })
                .catch(error => setError(error.response.data.detail || error.message || "Error"))
                .finally(() => {
                    setLoadingCsv(false);
                })
        }
    }

    return <>
        <LoadingOverlay active={loadingDatasets || loadingContent || loadingDependencies || loadingDataFilter || loadingCsv}/>
        {page === "baseTable" && (
            <SelectTableComponent
                tableInfos={tableInfos}
                nameFilter={nameFilter} setNameFilter={setNameFilter}
                nameRegExp={nameRegExp} setNameRegExp={setNameRegExp}
                dependenciesFilter={tableDependenciesFilter} setDependenciesFilter={setTableDependenciesFilter}
                tableName={rootTableName} setTableName={tableName => {
                setRootTableName(tableName)
                setDependency({
                    tableName,
                    selected: true,
                    constraintName: null,
                    subDependencies: [],
                    displayName: tableNameToFqName(tableName),
                    mandatory: true
                });
            }}
                setPage={setPage}
            />
        )}

        {page === "dependencies" && (
            <DependenciesComponent
                dependenciesFilter={dependenciesFilter} setDependenciesFilter={setDependenciesFilter}
                dependency={dependency!} setDependency={setSelectedDependency}
                onBack={() => setPage('baseTable')}
                onNext={() => {
                    setDataFilterChangeNumber(0);
                    setPage('dataFilter');
                    setSelectedDependency(dependency);
                }}
            />
        )}

        {page === "dataFilter" && (
            <DataFilterComponent setPage={setPage}
                                 datasets={datasets}
                                 dataset={dataset}
                                 setDataset={(p) => {
                                     setDataset(p);
                                     setDirty(true);
                                 }}
                                 autoRefresh={autoRefresh}
                                 setAutoRefresh={(b) => {
                                     setAutoRefresh(b);
                                     if (b && dirty) {
                                         refreshDataFilter();
                                     }
                                 }}
                                 refresh={refreshDataFilter}
                                 dirty={dirty} setDirty={setDirty}
                                 rowLimit={rowLimit} setRowLimit={setRowLimit}
                                 dependency={dependency!}
                                 dependencyQueries={dependencyQueries}
                                 setDependencyQueries={(dependencyQueries) => {
                                     setDependencyQueries(dependencyQueries);
                                     setDirty(true);
                                     if (autoRefresh) {
                                         refreshDataFilter();
                                     }
                                 }}
                                 previewResponse={previewResponse}
                                 onDownload={onDownload}
                                 error={error}
            />
        )}

        {page === "download-result" && (
            <DownloadResultsComponent downloadResponse={downloadResponse!}/>
        )}
    </>
}
