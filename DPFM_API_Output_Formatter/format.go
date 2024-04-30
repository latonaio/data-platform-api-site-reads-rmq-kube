package dpfm_api_output_formatter

import (
	"data-platform-api-site-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToHeader(rows *sql.Rows) (*[]Header, error) {
	defer rows.Close()
	header := make([]Header, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Header{}

		err := rows.Scan(
			&pm.Site,
			&pm.SiteType,
			&pm.SiteOwner,
			&pm.SiteOwnerBusinessPartnerRole,
			&pm.Brand,
			&pm.PersonResponsible,
			&pm.ValidityStartDate,
			&pm.ValidityStartTime,
			&pm.ValidityEndDate,
			&pm.ValidityEndTime,
			&pm.DailyOperationStartTime,
			&pm.DailyOperationEndTime,
			&pm.Description,
			&pm.LongText,
			&pm.Introduction,
			&pm.OperationRemarks,
			&pm.PhoneNumber,
			&pm.AvailabilityOfParking,
			&pm.NumberOfParkingSpaces,
			&pm.SuperiorSite,
			&pm.Project,
			&pm.WBSElement,
			&pm.Tag1,
			&pm.Tag2,
			&pm.Tag3,
			&pm.Tag4,
			&pm.PointConsumptionType,
			&pm.CreationDate,
			&pm.CreationTime,
			&pm.LastChangeDate,
			&pm.LastChangeTime,
			&pm.IsReleased,
			&pm.IsMarkedForDeletion,

		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &header, err
		}

		data := pm
		header = append(header, Header{
			Site:							data.Site,
			SiteType:						data.SiteType,
			SiteOwner:						data.SiteOwner,
			SiteOwnerBusinessPartnerRole:	data.SiteOwnerBusinessPartnerRole,
			Brand:							data.Brand,
			PersonResponsible:				data.PersonResponsible,
			ValidityStartDate:				data.ValidityStartDate,
			ValidityStartTime:				data.ValidityStartTime,
			ValidityEndDate:				data.ValidityEndDate,
			ValidityEndTime:				data.ValidityEndTime,
			DailyOperationStartTime:		data.DailyOperationStartTime,
			DailyOperationEndTime:			data.DailyOperationEndTime,
			Description:					data.Description,
			LongText:						data.LongText,
			Introduction:					data.Introduction,
			OperationRemarks:				data.OperationRemarks,
			PhoneNumber:					data.PhoneNumber,
			AvailabilityOfParking:			data.AvailabilityOfParking,
			NumberOfParkingSpaces:			data.NumberOfParkingSpaces,
			SuperiorSite:					data.SuperiorSite,
			Project:						data.Project,
			WBSElement:						data.WBSElement,
			Tag1:							data.Tag1,
			Tag2:							data.Tag2,
			Tag3:							data.Tag3,
			Tag4:							data.Tag4,
			PointConsumptionType:			data.PointConsumptionType,
			CreationDate:					data.CreationDate,
			CreationTime:					data.CreationTime,
			LastChangeDate:					data.LastChangeDate,
			LastChangeTime:					data.LastChangeTime,
			IsReleased:						data.IsReleased,
			IsMarkedForDeletion:			data.IsMarkedForDeletion,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &header, nil
	}

	return &header, nil
}

func ConvertToPartner(rows *sql.Rows) (*[]Partner, error) {
	defer rows.Close()
	partner := make([]Partner, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Partner{}

		err := rows.Scan(
			&pm.Site,
			&pm.PartnerFunction,
			&pm.BusinessPartner,
			&pm.BusinessPartnerFullName,
			&pm.BusinessPartnerName,
			&pm.Organization,
			&pm.Country,
			&pm.Language,
			&pm.Currency,
			&pm.ExternalDocumentID,
			&pm.AddressID,
			&pm.EmailAddress,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &partner, err
		}

		data := pm
		partner = append(partner, Partner{
			Site:                    data.Site,
			PartnerFunction:         data.PartnerFunction,
			BusinessPartner:         data.BusinessPartner,
			BusinessPartnerFullName: data.BusinessPartnerFullName,
			BusinessPartnerName:     data.BusinessPartnerName,
			Organization:            data.Organization,
			Country:                 data.Country,
			Language:                data.Language,
			Currency:                data.Currency,
			ExternalDocumentID:      data.ExternalDocumentID,
			AddressID:               data.AddressID,
			EmailAddress:            data.EmailAddress,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &partner, nil
	}

	return &partner, nil
}

func ConvertToAddress(rows *sql.Rows) (*[]Address, error) {
	defer rows.Close()
	address := make([]Address, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Address{}

		err := rows.Scan(
			&pm.Site,
			&pm.AddressID,
			&pm.PostalCode,
			&pm.LocalSubRegion,
			&pm.LocalRegion,
			&pm.Country,
			&pm.GlobalRegion,
			&pm.TimeZone,
			&pm.District,
			&pm.StreetName,
			&pm.CityName,
			&pm.Building,
			&pm.Floor,
			&pm.Room,
			&pm.XCoordinate,
			&pm.YCoordinate,
			&pm.ZCoordinate,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &address, err
		}

		data := pm
		address = append(address, Address{
			Site:        	data.Site,
			AddressID:   	data.AddressID,
			PostalCode:  	data.PostalCode,
			LocalSubRegion: data.LocalSubRegion,
			LocalRegion: 	data.LocalRegion,
			Country:     	data.Country,
			GlobalRegion: 	data.GlobalRegion,
			TimeZone:	 	data.TimeZone,
			District:    	data.District,
			StreetName:  	data.StreetName,
			CityName:    	data.CityName,
			Building:    	data.Building,
			Floor:       	data.Floor,
			Room:        	data.Room,
			XCoordinate: 	data.XCoordinate,
			YCoordinate: 	data.YCoordinate,
			ZCoordinate: 	data.ZCoordinate,
		})
	}
	if i == 0 {
		fmt.Printf("DBに対象のレコードが存在しません。")
		return &address, nil
	}

	return &address, nil
}
