import axios, {AxiosResponse} from "axios";

export interface SiteResponse {
    hasSource: boolean;
    hasTarget: boolean;
    message?: string;
}

export const DefaultSiteResponse = {hasSource: false, hasTarget: false};

export function siteApi(): Promise<AxiosResponse<SiteResponse>> {
    return axios.get<SiteResponse>('/site')
}