package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-site-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-site-reads-rmq-kube/DPFM_API_Output_Formatter"
	"fmt"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var header *[]dpfm_api_output_formatter.Header
	var address *[]dpfm_api_output_formatter.Address
	var partner *[]dpfm_api_output_formatter.Partner

	for _, fn := range accepter {
		switch fn {
		case "Header":
			func() {
				header = c.Header(mtx, input, output, errs, log)
			}()
		case "Headers":
			func() {
				header = c.Headers(mtx, input, output, errs, log)
			}()
		case "HeadersBySites":
			func() {
				header = c.HeadersBySites(mtx, input, output, errs, log)
			}()
		case "Partner":
			func() {
				partner = c.Partner(mtx, input, output, errs, log)
			}()
		case "Partners":
			func() {
				partner = c.Partners(mtx, input, output, errs, log)
			}()
		case "Address":
			func() {
				address = c.Address(mtx, input, output, errs, log)
			}()
		case "Addresses":
			func() {
				address = c.Addresses(mtx, input, output, errs, log)
			}()
		case "AddressesByLocalRegion":
			func() {
				address = c.AddressesByLocalRegion(mtx, input, output, errs, log)
			}()
		case "AddressesByLocalSubRegion":
			func() {
				address = c.AddressesByLocalSubRegion(mtx, input, output, errs, log)
			}()

		default:
		}
		if len(*errs) != 0 {
			break
		}
	}

	data := &dpfm_api_output_formatter.Message{
		Header:  header,
		Partner: partner,
		Address: address,
	}

	return data
}

func (c *DPFMAPICaller) Header(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	where := fmt.Sprintf("WHERE header.Site = %d", input.Header.Site)

	if input.Header.IsReleased != nil {
		where = fmt.Sprintf("%s\nAND header.IsReleased = %v", where, *input.Header.IsReleased)
	}

	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Site ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Headers(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	where := "WHERE 1 = 1"

	if input.Header.IsReleased != nil {
		where = fmt.Sprintf("%s\nAND header.IsReleased = %v", where, *input.Header.IsReleased)
	}

	if input.Header.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND header.IsMarkedForDeletion = %v", where, *input.Header.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Site ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) HeadersBySites(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Header {
	log.Info("HeadersBySites")
	in := ""

	for iHeader, vHeader := range input.Headers {
		site := vHeader.Site
		if iHeader == 0 {
			in = fmt.Sprintf(
				"( '%d' )",
				site,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%d' )",
			in,
			site,
		)
	}

	where := fmt.Sprintf(" WHERE ( Site ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_header_data AS header
		` + where + ` ORDER BY header.IsMarkedForDeletion ASC, header.IsReleased ASC, header.Site ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToHeader(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Partner(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Partner {
	var args []interface{}
	site := input.Header.Site
	partner := input.Header.Partner

	cnt := 0
	for _, v := range partner {
		args = append(args, site, v.PartnerFunction, v.BusinessPartner)
		cnt++
	}
	repeat := strings.Repeat("(?,?,?),", cnt-1) + "(?,?,?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_partner_data
		WHERE (Site, PartnerFunction, BusinessPartner) IN ( `+repeat+` ) 
		ORDER BY Site ASC, PartnerFunction ASC, BusinessPartner ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPartner(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Partners(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Partner {
	var args []interface{}
	site := input.Header.Site
	partner := input.Header.Partner

	cnt := 0
	for _, _ = range partner {
		args = append(args, site)
		cnt++
	}
	repeat := strings.Repeat("(?),", cnt-1) + "(?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_partner_data
		WHERE (Site) IN ( `+repeat+` ) 
		ORDER BY Site ASC, PartnerFunction ASC, BusinessPartner ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToPartner(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Address(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	var args []interface{}
	address := input.Header.Address

	cnt := 0
	for _, v := range address {
		args = append(args, v.Site, v.AddressID)
		cnt++
	}
	repeat := strings.Repeat("(?,?),", cnt-1) + "(?,?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_address_data
		WHERE (Site, AddressID) IN ( `+repeat+` ) 
		ORDER BY Site ASC, AddressID ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) Addresses(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	var args []interface{}
	site := input.Header.Site
	address := input.Header.Address

	cnt := 0
	for _, _ = range address {
		args = append(args, site)
		cnt++
	}
	repeat := strings.Repeat("(?),", cnt-1) + "(?)"

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_address_data
		WHERE (Site) IN ( `+repeat+` ) 
		ORDER BY Site ASC, AddressID ASC;`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalSubRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	where := "WHERE 1 = 1"

	if input.Header.Address[0].LocalSubRegion != nil {
		where = fmt.Sprintf("WHERE address.LocalSubRegion = \"%s\"", *input.Header.Address[0].LocalSubRegion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_address_data AS address
		` + where + ` ORDER BY address.LocalSubRegion ASC, address.Site ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalSubRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {

	log.Info("AddressesByLocalSubRegions")
	in := ""

	for iAddress, vAddress := range input.Header.Address {
		localSubRegion := vAddress.LocalSubRegion
		if iAddress == 0 {
			in = fmt.Sprintf(
				"( '%s' )",
				localSubRegion,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%s' )",
			in,
			localSubRegion,
		)
	}

	where := fmt.Sprintf(" WHERE ( LocalSubRegion ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_address_data AS address
		` + where + ` ORDER BY address.LocalSubRegion ASC, address.Site ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {
	where := "WHERE 1 = 1"

	if input.Header.Address[0].LocalRegion != nil {
		where = fmt.Sprintf("WHERE address.LocalRegion = \"%s\"", *input.Header.Address[0].LocalRegion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_address_data AS address
		` + where + ` ORDER BY address.LocalRegion ASC, address.Site ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) AddressesByLocalRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.Address {

	log.Info("AddressesByLocalRegions")
	in := ""

	for iAddress, vAddress := range input.Header.Address {
		localRegion := vAddress.LocalRegion
		if iAddress == 0 {
			in = fmt.Sprintf(
				"( '%s' )",
				localRegion,
			)
			continue
		}
		in = fmt.Sprintf(
			"%s ,( '%s' )",
			in,
			localRegion,
		)
	}

	where := fmt.Sprintf(" WHERE ( LocalRegion ) IN ( %s ) ", in)

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_site_address_data AS address
		` + where + ` ORDER BY address.LocalRegion ASC, address.Site ASC, address.AddressID ASC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToAddress(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
