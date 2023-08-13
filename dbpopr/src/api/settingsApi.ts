import axios, {AxiosResponse} from "axios";

export interface DatabaseConfigurationResponse {
    url?: string;
    username?: string;
    password?: string;
    fromEnvVariables: boolean;
}

export interface DatabaseConfigurationRequest {
    url?: string;
    username?: string;
    password?: string;
}

export interface Settings {
    sourceDatabaseConfiguration: DatabaseConfigurationResponse;
    targetDatabaseConfiguration: DatabaseConfigurationResponse;
}

export function getSettings(): Promise<AxiosResponse<Settings>> {
    return axios.get<Settings>('/settings')
}

export function postDatabase(type: string, databaseConfiguration: DatabaseConfigurationRequest) {
    return axios.post<DatabaseConfigurationResponse>(`/settings/${type}`, databaseConfiguration)
}