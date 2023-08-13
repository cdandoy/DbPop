import axios, {AxiosResponse} from "axios";

export interface DatabaseConfiguration {
    disabled: boolean;
    url: string;
    username: string;
    password: string;
    conflict: boolean;
}

export interface Settings {
    sourceDatabaseConfiguration: DatabaseConfiguration;
    targetDatabaseConfiguration: DatabaseConfiguration;
}

export function getSettings(): Promise<AxiosResponse<Settings>> {
    return axios.get<Settings>('/settings')
}

export function postDatabase(type:string, databaseConfiguration: DatabaseConfiguration) {
    return axios.post<DatabaseConfiguration>(`/settings/${type}`, databaseConfiguration)
}
export function postSource(databaseConfiguration: DatabaseConfiguration) {
    return axios.post<DatabaseConfiguration>(`/settings/source`, databaseConfiguration)
}

export function postTarget(databaseConfiguration: DatabaseConfiguration) {
    return axios.post<DatabaseConfiguration>(`/settings/target`, databaseConfiguration)
}