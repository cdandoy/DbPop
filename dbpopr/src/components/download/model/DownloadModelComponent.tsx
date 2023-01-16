import React, {useEffect, useState} from "react";
import SelectTableComponent from "./SelectTableComponent";
import {content, TableInfo} from "../../../api/content";
import LoadingOverlay from "../../utils/LoadingOverlay";
import DependenciesComponent, {DependenciesFilter} from "./DependenciesComponent";
import {Dependency} from "../../../models/Dependency";
import './DownloadModelComponent.scss'
import {TableName, tableNameToFqName} from "../../../models/TableName";
import dependenciesApi from "../../../api/dependenciesApi";
import DataFilterComponent from "./DataFilterComponent";
import DownloadResultsComponent from "./DownloadResultsComponent";
import {DownloadResponse} from "../../../models/DownloadResponse";
import datasetsApi from "../../../api/datasetsApi";

export default function DownloadModelComponent() {
    const [page, setPage] = useState("baseTable");
    const [loadingDatasets, setLoadingDatasets] = useState(false);
    const [loadingContent, setLoadingContent] = useState(false);
    const [loadingUniqueSchemas, setLoadingUniqueSchemas] = useState(false);
    const [loadingDependencies, setLoadingDependencies] = useState(false);
    const [tableInfos, setTableInfos] = useState<TableInfo[]>([])
    const [schemas, setSchemas] = useState<string[]>([])
    const [changeNumber, setChangeNumber] = useState(0);
    // Model
    const [rootTableName, setRootTableName] = useState<TableName | null>(null);
    const [dependency, setDependency] = useState<Dependency | null>(null);
    // SelectTableComponent filters
    const [schema, setSchema] = useState("");
    const [nameFilter, setNameFilter] = useState("");
    const [nameRegExp, setNameRegExp] = useState<RegExp>(/.*/);
    const [tableDependenciesFilter, setTableDependenciesFilter] = useState(true);
    // Dependencies filter
    const [dependenciesFilter, setDependenciesFilter] = useState<DependenciesFilter>({
        required: false,
        recommended: true,
        optional: false
    })
    // DataFilterComponent
    const [datasets, setDatasets] = useState<string[]>([])
    const [dataset, setDataset] = useState('static')
    const [downloadResponse, setDownloadResponse] = useState<DownloadResponse | undefined>();

    useEffect(() => {
        setLoadingDatasets(true);
        datasetsApi()
            .then(result => {
                setDatasets(result.data);
                setLoadingDatasets(false);
            })
    }, [])

    useEffect(() => {
        setLoadingContent(true);
        content()
            .then(result => {
                setTableInfos(result.data);
                setLoadingContent(false);
            })
    }, [changeNumber])

    useEffect(() => {
        setLoadingUniqueSchemas(true);
        const uniqueSchemas = new Set<string>();
        tableInfos
            .filter(tableInfo => !tableDependenciesFilter || tableInfo.dependencies.length > 0)
            .map(it => it.tableName.catalog + "." + it.tableName.schema).forEach(it => uniqueSchemas.add(it));
        setSchemas(Array.from(uniqueSchemas));
        setLoadingUniqueSchemas(false);
    }, [tableInfos, tableDependenciesFilter])

    useEffect(() => {
        if (dependency) {
            setLoadingDependencies(true)
            dependenciesApi(dependency)
                .then(result => {
                    setDependency(result.data);
                    setLoadingDependencies(false);
                });
        }
    }, [changeNumber])

    function recalculateDependencies() {
        setChangeNumber(changeNumber + 1);
    }

    return <>
        <LoadingOverlay active={loadingDatasets || loadingContent || loadingUniqueSchemas || loadingDependencies}/>
        {page === "baseTable" && (
            <SelectTableComponent
                schemas={schemas}
                tableInfos={tableInfos}
                schema={schema} setSchema={setSchema}
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
                recalculateDependencies();
            }
            }
                setPage={setPage}
            />
        )}
        {page === "dependencies" && (
            <DependenciesComponent
                dependenciesFilter={dependenciesFilter} setDependenciesFilter={setDependenciesFilter}
                dependency={dependency!} recalculateDependencies={recalculateDependencies}
                setPage={setPage}/>
        )}
        {page === "dataFilter" && (
            <DataFilterComponent setPage={setPage}
                                 datasets={datasets}
                                 dataset={dataset} setDataset={setDataset}
                                 dependency={dependency!} setDependency={setDependency}
                                 setDownloadResponse={setDownloadResponse}
            />
        )}
        {page === "download-result" && (
            <DownloadResultsComponent downloadResponse={downloadResponse!}/>
        )}
    </>
}
