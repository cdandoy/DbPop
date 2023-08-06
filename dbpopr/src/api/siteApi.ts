import axios, {AxiosResponse} from "axios";

export interface SiteStatus {
    hasSource: boolean;
    hasTarget: boolean;
    hasCode: boolean;
    codeChanges: number;
}

export const DefaultSiteStatus = {hasSource: false, hasTarget: false, hasCode: false, codeChanges: 0};

export function siteStatusApi(): Promise<AxiosResponse<SiteStatus>> {
    return axios.get<SiteStatus>('/site/status')
}