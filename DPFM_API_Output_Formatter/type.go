package dpfm_api_output_formatter

type SDC struct {
	ConnectionKey       string      `json:"connection_key"`
	RedisKey            string      `json:"redis_key"`
	Filepath            string      `json:"filepath"`
	APIStatusCode       int         `json:"api_status_code"`
	RuntimeSessionID    string      `json:"runtime_session_id"`
	BusinessPartnerID   *int        `json:"business_partner"`
	ServiceLabel        string      `json:"service_label"`
	APIType             string      `json:"api_type"`
	Message             interface{} `json:"message"`
	APISchema           string      `json:"api_schema"`
	Accepter            []string    `json:"accepter"`
	Deleted             bool        `json:"deleted"`
	SQLUpdateResult     *bool       `json:"sql_update_result"`
	SQLUpdateError      string      `json:"sql_update_error"`
	SubfuncResult       *bool       `json:"subfunc_result"`
	SubfuncError        string      `json:"subfunc_error"`
	ExconfResult        *bool       `json:"exconf_result"`
	ExconfError         string      `json:"exconf_error"`
	APIProcessingResult *bool       `json:"api_processing_result"`
	APIProcessingError  string      `json:"api_processing_error"`
}

type Message struct {
	Header             *[]Header	`json:"Header"`
	Partner            *[]Partner	`json:"Partner"`
	Address            *[]Address	`json:"Address"`
}

type Header struct {
	Site							int		`json:"Site"`
	SiteType						string	`json:"SiteType"`
	SiteOwner						int		`json:"SiteOwner"`
	SiteOwnerBusinessPartnerRole	string	`json:"SiteOwnerBusinessPartnerRole"`
	Brand							*int	`json:"Brand"`
	PersonResponsible				string	`json:"PersonResponsible"`
	ValidityStartDate				string	`json:"ValidityStartDate"`
	ValidityStartTime				string	`json:"ValidityStartTime"`
	ValidityEndDate					string	`json:"ValidityEndDate"`
	ValidityEndTime					string	`json:"ValidityEndTime"`
	DailyOperationStartTime			string	`json:"DailyOperationStartTime"`
	DailyOperationEndTime			string	`json:"DailyOperationEndTime"`
	Description						string	`json:"Description"`
	LongText						string	`json:"LongText"`
	Introduction					*string	`json:"Introduction"`
	OperationRemarks				*string	`json:"OperationRemarks"`
	PhoneNumber						*string	`json:"PhoneNumber"`
	AvailabilityOfParking			*bool	`json:"AvailabilityOfParking"`
	NumberOfParkingSpaces			*int	`json:"NumberOfParkingSpaces"`
	SuperiorSite					*int	`json:"SuperiorSite"`
	Project							*int	`json:"Project"`
	WBSElement						*int	`json:"WBSElement"`
	Tag1							*string	`json:"Tag1"`
	Tag2							*string	`json:"Tag2"`
	Tag3							*string	`json:"Tag3"`
	Tag4							*string	`json:"Tag4"`
	PointConsumptionType      		string  `json:"PointConsumptionType"`
	CreationDate					string	`json:"CreationDate"`
	CreationTime					string	`json:"CreationTime"`
	LastChangeDate					string	`json:"LastChangeDate"`
	LastChangeTime					string	`json:"LastChangeTime"`
	IsReleased						*bool	`json:"IsReleased"`
	IsMarkedForDeletion				*bool	`json:"IsMarkedForDeletion"`
}

type Partner struct {
	Site                 	int     `json:"Site"`
	PartnerFunction         string  `json:"PartnerFunction"`
	BusinessPartner         int     `json:"BusinessPartner"`
	BusinessPartnerFullName *string `json:"BusinessPartnerFullName"`
	BusinessPartnerName     *string `json:"BusinessPartnerName"`
	Organization            *string `json:"Organization"`
	Country                 *string `json:"Country"`
	Language                *string `json:"Language"`
	Currency                *string `json:"Currency"`
	ExternalDocumentID      *string `json:"ExternalDocumentID"`
	AddressID               *int    `json:"AddressID"`
	EmailAddress            *string `json:"EmailAddress"`
}

type Address struct {
	Site     		int     	`json:"Site"`
	AddressID   	int     	`json:"AddressID"`
	PostalCode  	string 		`json:"PostalCode"`
	LocalSubRegion 	string 		`json:"LocalSubRegion"`
	LocalRegion 	string 		`json:"LocalRegion"`
	Country     	string 		`json:"Country"`
	GlobalRegion   	string 		`json:"GlobalRegion"`
	TimeZone   		string 		`json:"TimeZone"`
	District    	*string 	`json:"District"`
	StreetName  	*string 	`json:"StreetName"`
	CityName    	*string 	`json:"CityName"`
	Building    	*string 	`json:"Building"`
	Floor       	*int		`json:"Floor"`
	Room        	*int		`json:"Room"`
	XCoordinate 	*float32	`json:"XCoordinate"`
	YCoordinate 	*float32	`json:"YCoordinate"`
	ZCoordinate 	*float32	`json:"ZCoordinate"`
}
