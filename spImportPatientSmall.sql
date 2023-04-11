USE [zrrx_synchronization_prototype]
GO
CREATE PROCEDURE [dbo].[spImportPatientSmall]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
 
  --BEGIN TRANSACTION TransactionPatient
    -- Insert statements for procedure here

    DECLARE @insuranceKPT bigint;
    SET @insuranceKPT = 7601003000382;

	------- Address import  --------------------------------------------

	select [ID_ERP], isnull(rtrim([Domicil_Salutation]), '') as [salutation], 
				isnull(rtrim([Domicil_Company]), '') as [company], isnull(rtrim([Domicil_Title]), '') as [title], 
				isnull(rtrim([Domicil_TitleCode]), '') as [titleCode], isnull(rtrim([Domicil_NameFamily]), '') as [familyName], 
				isnull(rtrim([Domicil_NameFirst]), '') as [firstName], 
				isnull(rtrim([Domicil_Street]), '') as [street], 
				isnull(rtrim([Domicil_AddressApp1]), '') as [addressLine1], isnull(rtrim([Domicil_AddressApp2]), '') as [addressLine2], 
				isnull(rtrim([Domicil_AddressApp3]), '') as [addressLine3], isnull(rtrim([Domicil_POBox]), '') as [POBox], 
				isnull(rtrim([Domicil_Zip]), '') as [zip], isnull(rtrim([Domicil_City]), '') as [city], isnull(rtrim([Domicil_Canton]), '') as [canton], 
				isnull(rtrim([Domicil_Country]), '') as [country], 
				isnull(rtrim([Domicil_PhonePrivate]), '') as [phonePrivate], isnull(rtrim([Domicil_PhoneBusiness]), '') as [phoneBusiness], 
				isnull(rtrim([Domicil_PhoneMobile]), '') as [phoneMobile], 
				isnull(rtrim([Domicil_Fax]), '') as [fax], [type],
				cast(null as int) as addressId,
				ROW_NUMBER() over (order by [ID_ERP]) as tmpId,
				DENSE_RANK() OVER (ORDER BY isnull(rtrim([Domicil_Salutation]), ''), 
					isnull(rtrim([Domicil_Company]), ''), isnull(rtrim([Domicil_Title]), ''), 
					isnull(rtrim([Domicil_TitleCode]), ''), isnull(rtrim([Domicil_NameFamily]), ''), 
					isnull(rtrim([Domicil_NameFirst]), ''), 
					isnull(rtrim([Domicil_Street]), ''), 
					isnull(rtrim([Domicil_AddressApp1]), '') , isnull(rtrim([Domicil_AddressApp2]), ''), 
					isnull(rtrim([Domicil_AddressApp3]), ''), isnull(rtrim([Domicil_POBox]), ''), 
					isnull(rtrim([Domicil_Zip]), ''), isnull(rtrim([Domicil_City]), '') , isnull(rtrim([Domicil_Canton]), ''), 
					isnull(rtrim([Domicil_Country]), '') , 
					isnull(rtrim([Domicil_PhonePrivate]), '') , isnull(rtrim([Domicil_PhoneBusiness]), '') , 
					isnull(rtrim([Domicil_PhoneMobile]), '') , 
					isnull(rtrim([Domicil_Fax]), '') ) AS addrGroup,
				rowCheckSum = CHECKSUM(
					nullif(rtrim([Domicil_Salutation]), ''),
					nullif(rtrim([Domicil_Company]), ''), 
					nullif(rtrim([Domicil_Title]), ''), 
					nullif(rtrim([Domicil_TitleCode]), ''), 
					nullif(rtrim([Domicil_NameFamily]), ''), 
					nullif(rtrim([Domicil_NameFirst]), ''), 
					nullif(rtrim([Domicil_Street]), ''), 
					nullif(rtrim([Domicil_AddressApp1]), ''), 
					nullif(rtrim([Domicil_AddressApp2]), ''), 
					nullif(rtrim([Domicil_AddressApp3]), ''), 
					nullif(rtrim([Domicil_POBox]), ''), 
					nullif(rtrim([Domicil_Zip]), ''), 
					nullif(rtrim([Domicil_City]), ''), 
					nullif(rtrim([Domicil_Canton]), ''), 
					nullif(rtrim([Domicil_Country]), ''), 
					nullif(rtrim([Domicil_PhonePrivate]), ''), 
					nullif(rtrim([Domicil_PhoneBusiness]), ''), 
					nullif(rtrim([Domicil_PhoneMobile]), ''), 
					nullif(rtrim([Domicil_Fax]), ''))
	into #tmpAddress
	from
	(
		select [ID_ERP], case when [Domicil_Salutation] in ('', 'Unknown') then '0' else [Domicil_Salutation] end as [Domicil_Salutation], 
				[Domicil_Company], [Domicil_Title], 
				[Domicil_TitleCode], [Domicil_NameFamily], [Domicil_NameFirst], [Domicil_Street], 
				[Domicil_AddressApp1], [Domicil_AddressApp2], [Domicil_AddressApp3], [Domicil_POBox], 
				[Domicil_Zip], [Domicil_City], [Domicil_Canton], [Domicil_Country], 
				[Domicil_PhonePrivate], [Domicil_PhoneBusiness], [Domicil_PhoneMobile], 
				[Domicil_Fax], 'DOMICIL' as [type]
		from
		(
			select [ID_ERP], [Domicil_Salutation], [Domicil_Company], [Domicil_Title], 
				[Domicil_TitleCode], [Domicil_NameFamily], [Domicil_NameFirst], [Domicil_Street], 
				[Domicil_AddressApp1], [Domicil_AddressApp2], [Domicil_AddressApp3], [Domicil_POBox], 
				[Domicil_Zip], [Domicil_City], [Domicil_Canton], [Domicil_Country], 
				[Domicil_PhonePrivate], [Domicil_PhoneBusiness], [Domicil_PhoneMobile], 
				[Domicil_Fax],
				ROW_NUMBER() over (partition by id_erp order by [Version] desc, ocDateCreate desc) as id  
			from [dbo].[tmpPatientImportTmp]
		) x
		where x.id = 1 
			and rtrim(isnull(x.[Domicil_Zip],'')) <> ''
			and rtrim(isnull(x.[Domicil_Street],'')) <> ''
		union all
		select [ID_ERP], case when [Delivery_Salutation] in ('', 'Unknown') then '0' else [Delivery_Salutation] end as [Delivery_Salutation], 
				[Delivery_Company], [Delivery_Title], 
				[Delivery_TitleCode], [Delivery_NameFamily], [Delivery_NameFirst], [Delivery_Street], 
				[Delivery_AddressApp1], [Delivery_AddressApp2], [Delivery_AddressApp3], [Delivery_POBox], 
				[Delivery_Zip], [Delivery_City], [Delivery_Canton], [Delivery_Country], 
				[Delivery_PhonePrivate], [Delivery_PhoneBusiness], [Delivery_PhoneMobile], 
				[Delivery_Fax], 'DELIVERY' as [type]
		from
		(
			select [ID_ERP], [Delivery_Salutation], [Delivery_Company], [Delivery_Title], 
				[Delivery_TitleCode], [Delivery_NameFamily], [Delivery_NameFirst], [Delivery_Street], 
				[Delivery_AddressApp1], [Delivery_AddressApp2], [Delivery_AddressApp3], [Delivery_POBox], 
				[Delivery_Zip], [Delivery_City], [Delivery_Canton], [Delivery_Country], 
				[Delivery_PhonePrivate], [Delivery_PhoneBusiness], [Delivery_PhoneMobile], 
				[Delivery_Fax],
				ROW_NUMBER() over (partition by id_erp order by [Version] desc, ocDateCreate desc) as id  
			from [dbo].[tmpPatientImportTmp]
		) x 
		where x.id = 1 
			and rtrim(isnull(x.[Delivery_Zip],'')) <> ''
			and rtrim(isnull(x.[Delivery_Street],'')) <> ''
		union all
		select [ID_ERP], case when [Bill_Salutation] in ('', 'Unknown') then '0' else [Bill_Salutation] end as [Bill_Salutation], 
				[Bill_Company], [Bill_Title], 
				[Bill_TitleCode], [Bill_NameFamily], [Bill_NameFirst], [Bill_Street], 
				[Bill_AddressApp1], [Bill_AddressApp2], [Bill_AddressApp3], [Bill_POBox], 
				[Bill_Zip], [Bill_City], [Bill_Canton], [Bill_Country], 
				[Bill_PhonePrivate], [Bill_PhoneBusiness], [Bill_PhoneMobile], 
				[Bill_Fax], 'BILL' as [type]
		from
		(
			select [ID_ERP], [Bill_Salutation], [Bill_Company], [Bill_Title], 
				[Bill_TitleCode], [Bill_NameFamily], [Bill_NameFirst], [Bill_Street], 
				[Bill_AddressApp1], [Bill_AddressApp2], [Bill_AddressApp3], [Bill_POBox], 
				[Bill_Zip], [Bill_City], [Bill_Canton], [Bill_Country], 
				[Bill_PhonePrivate], [Bill_PhoneBusiness], [Bill_PhoneMobile], 
				[Bill_Fax],
				ROW_NUMBER() over (partition by id_erp order by [Version] desc, ocDateCreate desc) as id  
			from [dbo].[tmpPatientImportTmp]
		) x 
		where x.id = 1 
			and rtrim(isnull(x.[Bill_Zip],'')) <> ''
			and rtrim(isnull(x.[Bill_Street],'')) <> ''
	) addr;
	
	update aImp
		set addressId = a.id
	from #tmpAddress aImp
		inner join (select max(tmpId) as tmpId, addrGroup from #tmpAddress group by addrGroup) g on aImp.tmpId = g.tmpId
		inner join [zrrx_prototype].[dbo].[Address] a on a.rowCheckSum = aImp.rowCheckSum
			and isnull(a.[salutation], '') = aImp.[salutation]
			and isnull(a.[company], '') = aImp.[company]
			and isnull(a.[title], '') = aImp.[title] 
			and isnull(a.[titleCode], '') = aImp.[titleCode]
			and	isnull(a.[familyName], '') = aImp.[familyName]
			and isnull(a.[firstName], '') = aImp.[firstName] 
			and isnull(a.[street], '') = aImp.[street]
			and isnull(a.[addressLine1], '') = aImp.[addressLine1]
			and isnull(a.[addressLine2], '') = aImp.[addressLine2]
			and isnull(a.[addressLine3], '') = aImp.[addressLine3]
			and isnull(a.[POBox], '') = aImp.[POBox]
			and isnull(a.[zip], '') = aImp.[zip]
			and isnull(a.[city], '') = aImp.[city]
			and isnull(a.[canton], '') = aImp.[canton]
			and isnull(a.[country], '') = aImp.[country] 
			and isnull(a.[phonePrivate], '') = aImp.[phonePrivate]
			and isnull(a.[phoneBusiness], '') = aImp.[phoneBusiness]
			and isnull(a.[phoneMobile], '') = aImp.[phoneMobile]
			and isnull(a.[fax], '') = aImp.[fax]

	update aImp
		set addressId = a.addressId
	from #tmpAddress aImp
		inner join (select addressId, addrGroup from #tmpAddress where not addressId is null) a on aImp.addrGroup = a.addrGroup
	where aImp.addressId is null

	if exists(select 1 from #tmpAddress where addressId is null)
	begin
		create table #tmp(id int, addrGroup int)

		MERGE [zrrx_prototype].[dbo].[Address] AS a --TARGET
		USING #tmpAddress aImp
			inner join (select max(tmpId) as tmpId, addrGroup from #tmpAddress group by addrGroup) g on aImp.tmpId = g.tmpId --SOURCE 
		ON (a.rowCheckSum = aImp.rowCheckSum
				and isnull(a.[salutation], '') = aImp.[salutation]
				and isnull(a.[company], '') = aImp.[company]
				and isnull(a.[title], '') = aImp.[title] 
				and isnull(a.[titleCode], '') = aImp.[titleCode]
				and	isnull(a.[familyName], '') = aImp.[familyName]
				and isnull(a.[firstName], '') = aImp.[firstName] 
				and isnull(a.[street], '') = aImp.[street]
				and isnull(a.[addressLine1], '') = aImp.[addressLine1]
				and isnull(a.[addressLine2], '') = aImp.[addressLine2]
				and isnull(a.[addressLine3], '') = aImp.[addressLine3]
				and isnull(a.[POBox], '') = aImp.[POBox]
				and isnull(a.[zip], '') = aImp.[zip]
				and isnull(a.[city], '') = aImp.[city]
				and isnull(a.[canton], '') = aImp.[canton]
				and isnull(a.[country], '') = aImp.[country] 
				and isnull(a.[phonePrivate], '') = aImp.[phonePrivate]
				and isnull(a.[phoneBusiness], '') = aImp.[phoneBusiness]
				and isnull(a.[phoneMobile], '') = aImp.[phoneMobile]
				and isnull(a.[fax], '') = aImp.[fax]) 
		WHEN NOT MATCHED BY TARGET THEN 
			insert 
				([salutation], [company], [title], 
				[titleCode], [familyName], [firstName], [street], 
				[addressLine1], [addressLine2], [addressLine3], [POBox], 
				[zip], [city], [canton], [country], 
				[phonePrivate], [phoneBusiness], [phoneMobile], 
				[fax], [active], [created], [rowCheckSum])
			VALUES (
				nullif([salutation], ''), nullif([company], ''), nullif([title], ''), 
				nullif([titleCode], ''), nullif([familyName], ''), nullif([firstName], ''), nullif([street], ''), 
				nullif([addressLine1], ''), nullif([addressLine2], ''), nullif([addressLine3], ''), nullif([POBox], ''), 
				nullif([zip], ''), nullif([city], ''), nullif([canton], ''), nullif([country], ''), 
				nullif([phonePrivate], ''), nullif([phoneBusiness], ''), nullif([phoneMobile], ''), 
				nullif([fax], ''), 1, getdate(), [rowCheckSum]
				)
		OUTPUT
			inserted.id, aImp.addrGroup
			into #tmp
		;

		update aImp
			set addressId = t.id
		from #tmpAddress aImp
			inner join #tmp t on aImp.addrGroup = t.addrGroup;
	end

	select DISTINCT TRY_CONVERT(int, x.id) as id, 
		nullif(rtrim(x.nameFamily), '') as nameFamily, 
		nullif(rtrim(x.[nameFirst]), '') as nameFirst, 
		nullif(rtrim(x.email), '') as email, 
		addrDOMICIL.addressId as domicilAddress, addrDELIVERY.addressId as deliveryAddress, addrBILL.addressId as billingAddress, 
		nullif(rtrim(x.sex), '') as sex, x.birthday, 
		nullif(rtrim(x.AHV), '') as AHV, 
		nullif(rtrim(x.socialInsuranceNr), '') as socialInsuranceNr, 
		nullif(rtrim(x.cardID), '') as cardID, 
		TRY_CONVERT(bigint, x.insuranceHealth) as insuranceHealth, x.insuranceHealthNr, 
		TRY_CONVERT(bigint, x.insuranceAddon) as insuranceAddon, x.insuranceAddonNr, 
		TRY_CONVERT(bigint, x.insuranceAccident) as insuranceAccident, x.insuranceAccidentNr, 
		nullif(rtrim(x.healthDisorder), '') as healthDisorder, 
		CASE WHEN lang.id is NOT NULL THEN lang.id ELSE defaultLanguage.id END as [language], 
    [version], 
		nullif(rtrim(x.remarks), '') as remarks, 
		x.active, x.status, 
		x.created, x.updated, x.deceased, x.createRemarks, x.updateRemarks,
		TRY_CONVERT(int, x.[ID_ERP_Merge]) as idMerge,
		[isDailyMedPatient],
		[serviceCareLevel]
		,[HealthQuestionnaire_Size]
		,[HealthQuestionnaire_Weight]
		,[HealthQuestionnaire_MedicalPreconditions_Diabetes]
		,[HealthQuestionnaire_MedicalPreconditions_BronchialAsthma]
		,[HealthQuestionnaire_MedicalPreconditions_KidneyDiseases]
		,[HealthQuestionnaire_MedicalPreconditions_Hypertension]
		,[HealthQuestionnaire_MedicalPreconditions_CardioVascularDiseases]
		,[HealthQuestionnaire_MedicalPreconditions_BleedingDisorder]
		,[HealthQuestionnaire_MedicalPreconditions_HepaticDiseases]
		,[HealthQuestionnaire_MedicalPreconditions_OtherDiseases]
		,[HealthQuestionnaire_CurrentDrugConsumptions]
		,[HealthQuestionnaire_Allergies_Penicillin]
		,[HealthQuestionnaire_Allergies_Sulphonamides]
		,[HealthQuestionnaire_Allergies_AcetylsalicylicAcid]
		,[HealthQuestionnaire_Allergies_Others]
		,[HealthQuestionnaire_IsPregnant]
		,[HealthQuestionnaire_IsBreastFeeding]
		,[HealthQuestionnaire_ExpectedChildDeliveryDate]
    ,[HealthQuestionnaire_Pharmacodes]
		,[CardIdHealth] as [cardIdHealth]
		,[Newsletter]
		,[ProspectOrigin]
		,[noAdvertising]
		,[inInstitution]
    ,[GenericMedicamentSubstitution]
		,case when [uNew].active = 1 then 'oravaMerge' 
			  when [uNew].active = 0 then 'oravaMergeInactive' 
			  else 'oravaRegistration' end as [action] --> newPatientId setzen
	into #tmpPatient
	from
	(
		select [ID_ERP] as id, Domicil_NameFamily as nameFamily, Domicil_NameFirst as [nameFirst], [Email] as email,
			null as domicilAddress, null as deliveryAddress, null as billingAddress,
			[Sex] as sex, 
			case when TRY_CONVERT(date, [DayOfBirth], 104) is null then 
				TRY_CAST([DayOfBirth] as date) 
			else 
				CONVERT(date, [DayOfBirth], 104) 
			end as birthday, 
			[AHV], [SocialInsuranceNr],
			[CardID], [InsuranceHealth], [InsuranceHealthNr], 
			[InsuranceAddon], [InsuranceAddonNr], [InsuranceAccident],
			[InsuranceAccidentNr], [HealthDisorder], [Language], 
			[Version],
			[Remarks], 
			isnull([ocIsActive], 0) as active,  --- value is allways null 
			isnull([ocStatus], 0) as status,	--- value is allways null 
			[ocDateCreate] as created, [ocDateUpdate] as updated, [ocDateEnd] as deceased, null as createRemarks, null as updateRemarks,
			[ID_ERP_Merge],
			[IsDailyMedPatient] as isDailyMedPatient,
			[ServiceCareLevel] as serviceCareLevel	
		    ,[HealthQuestionnaire_Size]
			,[HealthQuestionnaire_Weight]
			,[HealthQuestionnaire_MedicalPreconditions_Diabetes]
			,[HealthQuestionnaire_MedicalPreconditions_BronchialAsthma]
			,[HealthQuestionnaire_MedicalPreconditions_KidneyDiseases]
			,[HealthQuestionnaire_MedicalPreconditions_Hypertension]
			,[HealthQuestionnaire_MedicalPreconditions_CardioVascularDiseases]
			,[HealthQuestionnaire_MedicalPreconditions_BleedingDisorder]
			,[HealthQuestionnaire_MedicalPreconditions_HepaticDiseases]
			,[HealthQuestionnaire_MedicalPreconditions_OtherDiseases]
			,[HealthQuestionnaire_CurrentDrugConsumptions]
			,[HealthQuestionnaire_Allergies_Penicillin]
			,[HealthQuestionnaire_Allergies_Sulphonamides]
			,[HealthQuestionnaire_Allergies_AcetylsalicylicAcid]
			,[HealthQuestionnaire_Allergies_Others]
			,[HealthQuestionnaire_IsPregnant]
			,[HealthQuestionnaire_IsBreastFeeding]
			,[HealthQuestionnaire_ExpectedChildDeliveryDate]
      ,[HealthQuestionnaire_Pharmacodes]
			,[CardIdHealth]
			,[Newsletter]
			,[ProspectOrigin] 
			,[NoAdvertising] as noAdvertising
			,[InInstitution] as inInstitution
			,[GenericMedicamentSubstitution] as genericMedicamentSubstitution
			,ROW_NUMBER() over (partition by id_erp order by [Version] desc, ocDateCreate desc) as idx  
		from [dbo].[tmpPatientImportTmp]
	) x 
		left outer join #tmpAddress addrDOMICIL on x.id = addrDOMICIL.ID_ERP and addrDOMICIL.type = 'DOMICIL'
		left outer join #tmpAddress addrDELIVERY on x.id = addrDELIVERY.ID_ERP and addrDELIVERY.type = 'DELIVERY'
		left outer join #tmpAddress addrBILL on x.id = addrBILL.ID_ERP and addrBILL.type = 'BILL'
		left outer join [zrrx_prototype].[dbo].[Language] lang on SUBSTRING(x.Language, 1 ,2) = SUBSTRING(lang.name, 1 ,2)
		left join [zrrx_prototype].[dbo].[Language] defaultLanguage on defaultLanguage.iso = 'de'
		left join [zrrx_security_prototype].[dbo].[Users] uNew on [uNew].[customerId] = x.[ID_ERP_Merge]
	where x.idx = 1

	------ ??? do we need this reference to insurance ???? --------------------
	update p
		set InsuranceHealth = null
	from #tmpPatient p
		left outer join [zrrx_prototype].[dbo].[Insurance] o on p.InsuranceHealth = cast(o.ean as varchar(50))
	where not p.InsuranceHealth is null and o.ean is null

	update p
		set InsuranceAddon = null
	from #tmpPatient p
		left outer join [zrrx_prototype].[dbo].[Insurance] o on p.InsuranceAddon = cast(o.ean as varchar(50))
	where not p.InsuranceAddon is null and o.ean is null

	update p
		set InsuranceAccident = null
	from #tmpPatient p
		left outer join [zrrx_prototype].[dbo].[Insurance] o on p.InsuranceAccident = cast(o.ean as varchar(50))
	where not p.InsuranceAccident is null and o.ean is null


	MERGE [zrrx_prototype].[dbo].[Patient] AS p --TARGET
	USING #tmpPatient AS aImp --SOURCE 
	ON (p.id = aImp.id) 
	WHEN MATCHED THEN
		UPDATE SET [familyName] = aImp.[nameFamily], [firstName] = aImp.[nameFirst], [email] = aImp.[email],  
			[domicilAddress] = aImp.[domicilAddress], [deliveryAddress] = aImp.[deliveryAddress], [billingAddress] = aImp.[billingAddress], 
			[sex] = aImp.[sex], [birthday] = aImp.[birthday], [AHV] = aImp.[AHV], [socialInsuranceNr] = aImp.[socialInsuranceNr], 
			[cardID] = aImp.[cardID], [cardIDHealth] = aImp.[cardIdHealth], [insuranceHealth] = aImp.[insuranceHealth], [insuranceHealthNr] = aImp.[insuranceHealthNr], 
			[insuranceAddon] = aImp.[insuranceAddon], [insuranceAddonNr] = aImp.[insuranceAddonNr], 
			[insuranceAccident] = aImp.[insuranceAccident], [insuranceAccidentNr] = aImp.[insuranceAccidentNr], 
			[healthDisorder] = aImp.[healthDisorder], [language] = aImp.[language], 
			[remarks] = aImp.[remarks], 
			[active] = aImp.[active], [status] = aImp.[status],
			[updated] = aImp.[created], [deceased] = aImp.[deceased], [createRemarks] = aImp.[createRemarks], [updateRemarks] = aImp.[updateRemarks], [version] = aImp.[version],
			[isDailyMed] = aImp.[isDailyMedPatient], [serviceLevel] = aImp.[serviceCareLevel], [prospectOrigin] = aImp.[ProspectOrigin], [noAdvertising] = aImp.[noAdvertising], [inInstitution] = aImp.[inInstitution], [genericMedicamentSubstitution] = aImp.[genericMedicamentSubstitution],
      [syncDeclined] = case when  p.synced = 1 and p.insuranceHealth = @insuranceKPT and aImp.insuranceHealth != @insuranceKPT then 1 else p.syncDeclined end
	WHEN NOT MATCHED BY TARGET THEN 
		insert 
			(id, familyName, firstName, email, 
			domicilAddress, deliveryAddress, billingAddress, 
			sex, birthday, AHV, socialInsuranceNr, 
			cardID, insuranceHealth, insuranceHealthNr, 
			insuranceAddon, insuranceAddonNr, insuranceAccident, 
			insuranceAccidentNr, healthDisorder, [language], 
			remarks, [active], [status], 
			created, updated, deceased, createRemarks, updateRemarks, [version], [isDailyMed], [serviceLevel], [cardIDHealth], [prospectOrigin], [noAdvertising], [inInstitution], [genericMedicamentSubstitution])
		VALUES 
			(id, nameFamily, nameFirst, email, 
			domicilAddress, deliveryAddress, billingAddress, 
			sex, birthday, AHV, socialInsuranceNr, 
			cardID, insuranceHealth, insuranceHealthNr, 
			insuranceAddon, insuranceAddonNr, insuranceAccident, 
			insuranceAccidentNr, healthDisorder, [language], 
			remarks, [active], [status], 
			created, updated, deceased, createRemarks, updateRemarks, [version], [isDailyMedPatient], [serviceCareLevel], [cardIdHealth], [ProspectOrigin], [noAdvertising], [inInstitution], [genericMedicamentSubstitution])
	--WHEN NOT MATCHED BY SOURCE THEN 
	--DELETE
	;

	DECLARE @IdentityAllergiesTable TABLE (newIdentity INT, ts DATE)
	DECLARE @IdentityMedicalPreconditionsTable TABLE (newIdentity INT, ts DATE)

	/*
	Init empty HealthQuestions
	*/

	INSERT INTO [zrrx_prototype].[dbo].[MedicalPreconditions]  ([otherDiseases]) 
	OUTPUT inserted.id, GETDATE() INTO @IdentityMedicalPreconditionsTable(newIdentity, ts)
	SELECT NULL
	FROM #tmpPatient as pImp
	left join [zrrx_prototype].[dbo].[HealthQuestion] hq on hq.userId = pImp.id
	WHERE hq.userId is null


	INSERT INTO [zrrx_prototype].[dbo].[Allergies]  ([others]) 
	OUTPUT inserted.id, GETDATE() INTO @IdentityAllergiesTable(newIdentity, ts)
	SELECT NULL
	FROM #tmpPatient as pImp
	left join [zrrx_prototype].[dbo].[HealthQuestion] hq on hq.userId = pImp.id
	WHERE hq.userId is null

	INSERT INTO [zrrx_prototype].[dbo].[HealthQuestion] ([userId], [medicalPreconditions], [allergies], [creationDate])
	SELECT patients.id, preconditionsTable.newIdentity as [medicalPreconditions], allergiesTable.newIdentity as allergies, GETDATE() FROM 
	(select *, ROW_NUMBER() OVER(ORDER BY ts DESC) as rn from @IdentityMedicalPreconditionsTable) preconditionsTable
	left join (select *, ROW_NUMBER() OVER(ORDER BY ts DESC) as rn from @IdentityAllergiesTable) allergiesTable on allergiesTable.rn = preconditionsTable.rn
	left join (select pImp.id, ROW_NUMBER() OVER(ORDER BY pImp.id DESC) as rn FROM #tmpPatient as pImp left join [zrrx_prototype].[dbo].[HealthQuestion] hq on hq.userId = pImp.id	WHERE hq.userId is null) patients on patients.rn = preconditionsTable.rn

	/**
	UPDATE health questions
	**/

	MERGE [zrrx_prototype].[dbo].[HealthQuestion] AS hq --TARGET
	USING #tmpPatient AS aImp --SOURCE 
	ON (hq.userId = aImp.id) 
	WHEN MATCHED THEN
		UPDATE SET 
		[size] = aImp.[HealthQuestionnaire_Size], 
		[weight] = aImp.[HealthQuestionnaire_Weight],
    [pharmaCodes] = aImp.[HealthQuestionnaire_Pharmacodes],
	--	[currentDrugConsumption] = aImp.[HealthQuestionnaire_CurrentDrugConsumptions],  
		[expectedChildDeliveryDate] = aImp.[HealthQuestionnaire_ExpectedChildDeliveryDate],
		[isAllergic] = (CASE  
			WHEN  aImp.[HealthQuestionnaire_Allergies_Penicillin] = 1 THEN 1 
			WHEN  aImp.[HealthQuestionnaire_Allergies_Sulphonamides] = 1 THEN 1 
			WHEN  aImp.[HealthQuestionnaire_Allergies_AcetylsalicylicAcid] = 1 THEN 1 
			WHEN  aImp.[HealthQuestionnaire_Allergies_Others] is not null and aImp.[HealthQuestionnaire_Allergies_Others] != '' THEN 1 
			ELSE 0 
			END  
		) ,
		[isPregnant] = aImp.[HealthQuestionnaire_IsPregnant], 
		[isBreastfeeding] = aImp.[HealthQuestionnaire_IsBreastFeeding];


	MERGE [zrrx_prototype].[dbo].[Allergies] AS a --TARGET
	USING (SELECT 
		hq.allergies
		,aImp.[HealthQuestionnaire_Allergies_Penicillin]
		,aImp.[HealthQuestionnaire_Allergies_Sulphonamides]
		,aImp.[HealthQuestionnaire_Allergies_AcetylsalicylicAcid]
		,aImp.[HealthQuestionnaire_Allergies_Others]
       FROM #tmpPatient aImp
      		inner join [zrrx_prototype].[dbo].[HealthQuestion] hq on hq.[userId] = aImp.id) tmpTable --SOURCE
	ON (tmpTable.allergies = a.id) 
	WHEN MATCHED THEN
		UPDATE SET 
		[penicillin] = tmpTable.[HealthQuestionnaire_Allergies_Penicillin]
      ,[sulphonamides] = tmpTable.[HealthQuestionnaire_Allergies_Sulphonamides]
      ,[acetylsalicylicAcid] = tmpTable.[HealthQuestionnaire_Allergies_AcetylsalicylicAcid]
      ,[others] = tmpTable.[HealthQuestionnaire_Allergies_Others];


	
	MERGE [zrrx_prototype].[dbo].MedicalPreconditions AS mp --TARGET
	USING (SELECT 
		hq.[medicalPreconditions]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_Diabetes]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_BronchialAsthma]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_KidneyDiseases]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_Hypertension]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_CardioVascularDiseases]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_BleedingDisorder]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_HepaticDiseases]
		,aImp.[HealthQuestionnaire_MedicalPreconditions_OtherDiseases]
       FROM #tmpPatient aImp
      		inner join [zrrx_prototype].[dbo].[HealthQuestion] hq on hq.[userId] = aImp.id) tmpTable --SOURCE
	ON (tmpTable.[medicalPreconditions] = mp.id) 
	WHEN MATCHED THEN
		UPDATE SET 
		 [diabetes] = tmpTable.[HealthQuestionnaire_MedicalPreconditions_Diabetes]
		,[bronchialAsthma]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_BronchialAsthma]
		,[kidneyDiseases]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_KidneyDiseases]
		,[hypertension]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_Hypertension]
		,[cardioVascularDiseases]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_CardioVascularDiseases]
		,[bleedingDisorder]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_BleedingDisorder]
		,[hepaticDiseases]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_HepaticDiseases]
		,[otherDiseases]= tmpTable.[HealthQuestionnaire_MedicalPreconditions_OtherDiseases]	;


	MERGE [zrrx_prototype].[dbo].[PatientAddress] AS pa --TARGET
	USING [zrrx_prototype].[dbo].[Patient] p
		inner join #tmpAddress a on p.id = a.ID_ERP  --SOURCE 
	ON (pa.[patient] = p.id and pa.[address] = a.addressId and pa.[type] = a.[type]) 
	WHEN NOT MATCHED BY TARGET THEN 
		insert 
			(patient, [address], [type])
		VALUES 
			(p.id, a.addressId, a.[type])	
	--WHEN NOT MATCHED BY SOURCE THEN 
	--DELETE
	;


	MERGE [zrrx_prototype].[dbo].[PatientConfiguration] AS pc --TARGET
  	USING (SELECT 
		aImp.id as id,
		(CASE  
			WHEN  aImp.[Newsletter] = 'Yes' THEN 1 
			WHEN  aImp.[Newsletter] = 'No' THEN 0 
			ELSE NULL
			END  
		) as newsletter
       FROM #tmpPatient aImp) tmpTable --SOURCE
	ON (pc.[patientId] = tmpTable.id) 
	WHEN MATCHED AND tmpTable.newsletter is not null THEN
		UPDATE SET 
		[receiveNewsletter] = tmpTable.newsletter,
		[updated] = getdate(),
    [modifiedBy] = 1
	WHEN NOT MATCHED BY TARGET THEN 
		insert 
			([patientId], [notificationSms], [notificationEmail], [updated], [receiveNewsletter], [modifiedBy])
		VALUES 
			(tmpTable.id, 0, 0, getdate(), tmpTable.newsletter, 1);	


	if exists(select 1 from #tmpPatient where idMerge > 0)
	begin

		update o
			set [patient] = idMerge
		from [zrrx_prototype].[dbo].[Order] o
			inner join #tmpPatient p on o.patient = p.id
		where idMerge > 0

		update pre
			set [patient] = idMerge
		from [zrrx_prototype].[dbo].[Prescription] pre
			inner join #tmpPatient p on pre.patient = p.id
		where idMerge > 0

	-- when [uNew].active = 1 then 'oravaMerge' 
	-- when [uNew].active = 0 then 'oravaMergeInactive' 
	-- else 'oravaRegistration' end as [action] --> newPatientId setzen

  -- update syncDeclined of the old patient if he has an active data transfer
	-- and the new patient is not insuraced KPT	then syncDeclined client to 1
	update [p]
		set
		 [syncDeclined] = case when p.synced = 1 and p.insuranceHealth = @insuranceKPT 
		 and [Patient].insuranceHealth != @insuranceKPT then 1 else p.syncDeclined end
	from [zrrx_prototype].[dbo].[Patient] [Patient]
    inner join #tmpPatient p2 on [Patient].id = p2.[idMerge] --newPatientId
	left join [zrrx_prototype].[dbo].[Patient] p on p.id = p2.id --oldPatientId
	where p2.idMerge > 0

    -- daten von deaktiviertem user übernehmen
	update [Patient]
		set
		  [source] = CASE WHEN p.[source] IS NULL OR ([Patient].[source] IS NOT NULL AND [Patient].[source] != 'portal') THEN [Patient].[source] ELSE p.[source] END, -- keep target.source, when is not null, so that the values portal and especially kpt won't be overriden
      [synced] = CASE WHEN [Patient].[synced] IS NULL THEN p.[synced] ELSE [Patient].[synced] END,
			[syncDeclined] = CASE WHEN p.[syncDeclined] IS NULL OR ([Patient].[syncDeclined] IS NOT NULL AND [Patient].[syncDeclined] != 0) THEN [Patient].[syncDeclined] ELSE p.[syncDeclined] END,
      [prospectOrigin] = CASE WHEN [Patient].[prospectOrigin] IS NULL THEN p.[prospectOrigin] ELSE [Patient].[prospectOrigin] END,
      [agreedToTermsAndConditions] = CASE WHEN [Patient].[agreedToTermsAndConditions] IS NULL OR (p.[agreedToTermsAndConditions] IS NOT NULL AND [Patient].[agreedToTermsAndConditions] > p.[agreedToTermsAndConditions]) THEN p.[agreedToTermsAndConditions] ELSE [Patient].[agreedToTermsAndConditions] END,
			[cardIDHealth] = p.[cardIDHealth]
	from [zrrx_prototype].[dbo].[Patient] [Patient]
    inner join #tmpPatient p2 on [Patient].id = p2.[idMerge] --newPatientId
	left join [zrrx_prototype].[dbo].[Patient] p on p.id = p2.id --oldPatientId
	where p2.idMerge > 0
  	
	-- deactivate old Patient 
	  update [Users] 
		set [active] = 0
	from [zrrx_security_prototype].[dbo].[Users] 
    inner join #tmpPatient p on [Users].customerId = p.[id]
	where p.idMerge > 0

    -- delete HealthQuestion from 'old, deactivated' patients or 'older HealthQuestions'
    delete [hq] 
    from [zrrx_prototype].[dbo].[HealthQuestion] [hq]
    inner join #tmpPatient p on [hq].userId = p.idMerge --oravaId
    left join [zrrx_prototype].[dbo].[HealthQuestion] [hq2] on [hq2].userId = p.[id] -- existing patientId
    where p.idMerge > 0 and ([action] = 'oravaMergeInactive' or [action] = 'oravaRegistration'
    or (p.[action] = 'oravaMerge' and [hq].creationDate <= [hq2].creationDate)) -- wenn orava HealthQuestion älter als die des existierenden, dann löschen

-- migrate healthQuestion
  update [h1] 
		set [userId] = p.idMerge
	from [zrrx_prototype].[dbo].[HealthQuestion] h1
    inner join #tmpPatient p on h1.userId = p.[id]
    left join [zrrx_prototype].[dbo].[HealthQuestion] h2 on h2.userId = p.[idMerge]
  where  p.idMerge > 0 and ((p.[action] = 'oravaMerge' and [h2].userId is null) 
    or p.[action] = 'oravaMergeInactive'
    or p.[action] = 'oravaRegistration')

-- set action for later processing
  update [Patient]
		set [actionRequired] = p.[action]
	from [zrrx_prototype].[dbo].[Patient] [Patient]
    inner join #tmpPatient p on [Patient].id = p.[id]
	where p.idMerge > 0 and ([Patient].[idMerge] is null or (p.idMerge != TRY_CONVERT(int, [Patient].[idMerge])))

		update [p]
      set [tasks] = case when p.[tasks] is null then 0 else p.[tasks] end | 8
    from [zrrx_prototype].[dbo].[Patient] [p]
     inner join #tmpPatient t on [p].id = t.[idMerge] --newPatientId
	   left join [zrrx_prototype].[dbo].[Patient] [p3] on [p3].id = t.id --oldPatientId      
    where t.[idMerge] > 0 and ([p3].[idMerge] is null or (t.[idMerge] != TRY_CONVERT(int, [p3].[idMerge])))
	
	-- Übertragung von Eventdaten (doppelte werden vermieden, daher nur oravaMergeInactive und oravaRegistration
	  update [ped]
			set [patientId] = p.idMerge
		from [zrrx_prototype].[dbo].[PatientEventData] [ped]
      inner join #tmpPatient p on [ped].patientId = p.id
	  where p.idMerge > 0 and ( p.[action] = 'oravaMergeInactive' or p.[action] = 'oravaRegistration');

    update [p]
      set [p].[idMerge] = t.idMerge, [p].[updated] = GETDATE()
    from [zrrx_prototype].[dbo].[Patient] [p]
      inner join #tmpPatient t on [p].id = t.id
	  where t.idMerge > 0
	  
  -- BEGINN: BDPAS-2862 
  -- Wenn Konfiguration für Orava Kundennummer vorhanden, dann NUR KuKo Daten auf neuen Datensatz mit Orava Kundennummer übertragen und alten Datensatz löschen
    UPDATE [pc] SET -- Orava Kunden updaten
      updated = SYSUTCDATETIME(),
      receiveNewsletter = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveNewsletter ELSE pc.receiveNewsletter END,
      notificationSms = CASE WHEN pc2.updated > pc.updated THEN pc2.notificationSms ELSE pc.notificationSms END,
      notificationEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.notificationEmail ELSE pc.notificationEmail END,
      receiveReminderEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderEmail ELSE pc.receiveReminderEmail END,
      receiveReminderAltEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderAltEmail ELSE pc.receiveReminderAltEmail END,
      receiveReminderSms = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderSms ELSE pc.receiveReminderSms END,
      receiveReminderAltMobileNumber = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderAltMobileNumber ELSE pc.receiveReminderAltMobileNumber END,
      receiveReminderTimeBeforeIntakeEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderTimeBeforeIntakeEmail ELSE pc.receiveReminderTimeBeforeIntakeEmail END,
      receiveReminderTimeBeforeIntakeSms = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderTimeBeforeIntakeSms ELSE pc.receiveReminderTimeBeforeIntakeSms END,
      receiveReminderTimeAfterIntakeEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderTimeAfterIntakeEmail ELSE pc.receiveReminderTimeAfterIntakeEmail END,
      receiveReminderTimeAfterIntakeSms = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderTimeAfterIntakeSms ELSE pc.receiveReminderTimeAfterIntakeSms END,
      modifiedBy = 3, -- id for merge
      receivePrescriptionRequestEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receivePrescriptionRequestEmail ELSE pc.receivePrescriptionRequestEmail END,
      receivePrescriptionRequestSms = CASE WHEN pc2.updated > pc.updated THEN pc2.receivePrescriptionRequestSms ELSE pc.receivePrescriptionRequestSms END,
      receiveReminderOrderEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderOrderEmail ELSE pc.receiveReminderOrderEmail END,
      receiveReminderOrderSms = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveReminderOrderSms ELSE pc.receiveReminderOrderSms END,
      receiveMarketingAutomationInformation = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveMarketingAutomationInformation ELSE pc.receiveMarketingAutomationInformation END,
      receiveMarketingAutomationInformationTimestamp = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveMarketingAutomationInformationTimestamp ELSE pc.receiveMarketingAutomationInformationTimestamp END,
      receiveWeightTrackingReminderIntervalStartDate = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveWeightTrackingReminderIntervalStartDate ELSE pc.receiveWeightTrackingReminderIntervalStartDate END,
      receiveWeightTrackingReminderIntervalDays = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveWeightTrackingReminderIntervalDays ELSE pc.receiveWeightTrackingReminderIntervalDays END,
      receiveWeightTrackingReminderEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveWeightTrackingReminderEmail ELSE pc.receiveWeightTrackingReminderEmail END,
      receiveWeightTrackingReminderSms = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveWeightTrackingReminderSms ELSE pc.receiveWeightTrackingReminderSms END,
      receiveWeightTrackingReminderAltEmail = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveWeightTrackingReminderAltEmail ELSE pc.receiveWeightTrackingReminderAltEmail END,
      receiveWeightTrackingReminderAltMobileNumber = CASE WHEN pc2.updated > pc.updated THEN pc2.receiveWeightTrackingReminderAltMobileNumber ELSE pc.receiveWeightTrackingReminderAltMobileNumber END
    FROM [zrrx_prototype].[dbo].[PatientConfiguration] [pc] -- Orava Kunde
      INNER JOIN #tmpPatient p ON [pc].patientId = p.idMerge 
      JOIN [zrrx_prototype].[dbo].[PatientConfiguration] [pc2] ON [pc2].patientId = p.id -- Lean Kunde
    WHERE (p.[action] = 'merge' AND [pc].patientId IS NOT NULL) 
      OR p.[action] = 'oravaOnly'
      OR p.[action] = 'mergeInactive' and idMerge > 0;

    DELETE [pc2] -- Lean Kunden löschen
    FROM [zrrx_prototype].[dbo].[PatientConfiguration] [pc] -- leading Orava customer
      INNER JOIN #tmpPatient p ON [pc].patientId = p.idMerge 
      JOIN [zrrx_prototype].[dbo].[PatientConfiguration] [pc2] ON [pc2].patientId = p.id -- 'old' customer
    WHERE (p.[action] = 'merge' AND [pc].patientId IS NOT NULL) 
      OR p.[action] = 'oravaOnly'
      OR p.[action] = 'mergeInactive' and idMerge > 0;

    -- ENDE: Wenn Konfiguration für Orava Kundennummer vorhanden, dann NUR KuKo Daten auf neuen Datensatz mit Orava Kundennummer übertragen und alten Datensatz löschen

	-- delete PatientConfiguration from 'old, deactivated' patients
    delete [pc] 
    from [zrrx_prototype].[dbo].[PatientConfiguration] [pc]
    inner join #tmpPatient p on [pc].patientId = p.idMerge --oravaId
    left join [zrrx_prototype].[dbo].[PatientConfiguration] [pc2] on [pc2].patientId = p.[id] -- existing patientId
    where p.idMerge > 0 and ([action] = 'oravaMergeInactive' or [action] = 'oravaRegistration'
    or (p.[action] = 'oravaMerge' and [pc].updated < [pc2].updated)) -- wenn orava PatientConfiguration älter als die des existierenden, dann löschen

	-- Übertragung von Konfiguration, wenn oravaMergeInactive oder oravaMerge und patientconfiguration nicht existent
	update [pc]
		set [patientId] = p.idMerge
		from [zrrx_prototype].[dbo].[PatientConfiguration] [pc]
      inner join #tmpPatient p on [pc].patientId = p.id 
      left join [zrrx_prototype].[dbo].[PatientConfiguration] [pc2] on [pc2].patientId = p.[idMerge]
	  where p.idMerge > 0 and ((p.[action] = 'oravaMerge' and [pc2].patientId is null) 
    or p.[action] = 'oravaMergeInactive'
    or p.[action] = 'oravaRegistration');

    -- setzen der customerId in Users auf newPatientId für notExistent, um userName und Passwort zu übernehmen
    update [Users]
			set [customerId] = p.idMerge
		from [zrrx_security_prototype].[dbo].[Users] [Users]
      inner join #tmpPatient p on [Users].customerId = p.id
    where p.idMerge > 0 and p.[action] = 'oravaRegistration'

        -- setzen des accountNames in Users auf idMerge for notExistant, wenn vorheriger AccountName = 'alte ID' 
    update [Users]
      set [accountName] = p.idMerge
    from [zrrx_security_prototype].[dbo].[Users] [Users]
      inner join #tmpPatient p on [Users].customerId = p.idMerge
    where p.idMerge > 0 and (p.[action] = 'oravaRegistration' and ISNUMERIC(CAST([accountName] as xml).value('. cast as xs:decimal?','int')) = 1)


    -- 'swap' der Id's 
    if exists(select 1 from #tmpPatient)
    BEGIN

        declare @id int
        declare @idMerge int
        declare @accountName nvarchar(200)
        declare cur CURSOR LOCAL for
            select p.id, p.idMerge, u.accountName from #tmpPatient p
            left join [zrrx_security_prototype].[dbo].[Users] u on u.customerId = p.id 
            where [action] = 'oravaMergeInactive'

        open cur

        fetch next from cur into @id, @idMerge, @accountName

        while @@FETCH_STATUS = 0 BEGIN

            --execute your sproc on each row
            exec [SwapIds] @id, @idMerge

            -- check if accountName is old patientId. If so, change it to new PatientId
            if ISNUMERIC(CAST(@accountName as xml).value('. cast as xs:decimal?','int')) = 1
            BEGIN
              update [zrrx_security_prototype].[dbo].[Users] set accountName = @idMerge where customerId = @idMerge
            END

            fetch next from cur into @id, @idMerge, @accountName
        END

        close cur
        deallocate cur
    END

    	-- activate new but deactived Patient 
	  update [Users] 
      set [active] = 1
		from [zrrx_security_prototype].[dbo].[Users] 
		  inner join #tmpPatient p on [Users].customerId = p.[idMerge]
		where p.idMerge > 0 and (p.[action] = 'oravaMergeInactive' or p.[action] = 'oravaRegistration')

  end

  MERGE [zrrx_prototype].[dbo].[Patient] AS p --TARGET
  USING (SELECT DISTINCT
      p1.id AS sourceId,
      p1.cardID AS sourceCardId,
      p1.insuranceHealth AS sourceInsuranceHealth
    FROM [zrrx_prototype].[dbo].[Patient] p1
    LEFT OUTER JOIN [zrrx_prototype].[dbo].[Patient] p2 ON p1.id = p2.idMerge
    WHERE
      p1.insuranceHealth != @insuranceKPT
      AND p1.syncDeclined = 1
      AND (p2.syncDeclined = 0 OR p2.syncDeclined is null)
  ) AS aImp --SOURCE 
	ON (p.idMerge = aImp.sourceId) 
	WHEN MATCHED THEN
	    UPDATE SET 
	    [cardID] = aImp.[sourceCardId], 
	    [insuranceHealth] = aImp.[sourceInsuranceHealth],
	    [syncDeclined] = 1;

	UPDATE i
		SET ocStatus = -999
	FROM #tmpPatient p
		INNER JOIN [zrrx_synchronization_prototype].[dbo].[tmpPatientImport] i ON p.id = i.ID_ERP and p.created >= i.ocDateCreate 


	DELETE p FROM [dbo].[tmpPatientImportTmp] p  	
	inner join [zrrx_synchronization_prototype].[dbo].[tmpPatientImport] i on p.ID_ERP = i.ID_ERP and p.ocDateCreate = i.ocDateCreate 
	WHERE i.ocStatus = -999

	drop table #tmpAddress;
	drop table #tmpPatient;

	if(SELECT count(*) from [zrrx_synchronization_prototype].[dbo].[tmpPatientImport]) > 1000
	begin
		exec [zrrx_synchronization_prototype].[dbo].[spCopyToArchivePatient];
		exec [zrrx_synchronization_prototype].[dbo].[spCopyToArchiveInsurance];
	end

	--COMMIT
END
GO
