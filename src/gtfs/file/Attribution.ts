export interface Attribution {
    attribution_id?: string;
    agency_id?: string;
    route_id?: string;
    trip_id?: string;
    organization_name: string;
    is_producer?: 0 | 1;
    is_operator?: 0 | 1;
    is_authority?: 0 | 1;
    attribution_url?: string;
    attribution_email?: string;
    attribution_phone?: string;
}