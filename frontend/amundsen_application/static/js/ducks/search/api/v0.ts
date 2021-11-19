import axios, { AxiosResponse } from 'axios';

import {
  indexDashboardsEnabled,
  indexFeaturesEnabled,
  indexUsersEnabled,
} from 'config/config-utils';
import { ResourceType, SearchType } from 'interfaces';

import {
  DashboardSearchResults,
  FeatureSearchResults,
  TableSearchResults,
  UserSearchResults,
} from '../types';

import { FilterReducerState, ResourceFilterReducerState } from '../filters/reducer';

export const BASE_URL = '/api/search/v1';

const RESOURCE_TYPES = ['dashboards', 'features', 'tables', 'users'];

export interface SearchAPI {
  msg: string;
  status_code: number;
  search_term: string;
  dashboards?: DashboardSearchResults;
  features?: FeatureSearchResults;
  tables?: TableSearchResults;
  users?: UserSearchResults;
}

export interface Filters {
  [categoryId: string]: [values: string[]]
}

export const searchHelper = (response: AxiosResponse<SearchAPI>) => {
  const { data } = response;
  const ret = { searchTerm: data.search_term };
  RESOURCE_TYPES.forEach((key) => {
    if (data[key]) {
      ret[key] = data[key];
    }
  });
  return ret;
};

export const isResourceIndexed = (resource: ResourceType) => {
  // table is always configured
  if (resource === ResourceType.table) {
    return true;
  }
  if (resource === ResourceType.user) {
    return indexUsersEnabled();
  }
  if (resource === ResourceType.dashboard) {
    return indexDashboardsEnabled();
  }
  if (resource === ResourceType.feature) {
    return indexFeaturesEnabled();
  }
  return false;
};

export function search(
  pageIndex: number,
  resultsPerPage: number,
  resources: ResourceType[],
  searchTerm: string,
  filters: ResourceFilterReducerState | FilterReducerState = {},
  searchType: SearchType
) {
  // If given invalid resource in list dont search for that one only for valid ones
    resources = resources.filter(r => isResourceIndexed(r));
    if (resources.length < 1) {
      // If there are no resources to search through then return {}
      return Promise.resolve({});
    }
  // TODO change filters to desired format
  console.log(filters);

  return axios
      .post(`${BASE_URL}/search`, {
        filters,
        pageIndex,
        resources,
        resultsPerPage,
        searchTerm,
        searchType,
      })
      .then(searchHelper);
}
