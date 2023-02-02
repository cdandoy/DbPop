import axios, {AxiosResponse} from "axios";

export interface SiteResponse {
    hasSource: boolean;
    hasTarget: boolean;
    featureFlagCode: boolean;
}

export const DefaultSiteResponse = {hasSource: false, hasTarget: false, featureFlagCode: false};

export function siteApi(): Promise<AxiosResponse<SiteResponse>> {
    return axios.get<SiteResponse>('/site')
}