FILTERMAPDRAWER_QUERY = """
query filterMapDrawer(
  $activeLayers: [MapLayers]!
  $creaListingsInput: FilterMapChildInput
  $propertiesInput: FilterMapChildInput
) {
  filterMap(activeLayers: $activeLayers) {
    creaListings(input: $creaListingsInput) {
      results {
        ...CreaListingCard
      }
      count
    }
    properties(input: $propertiesInput) {
      results {
        ...PropertyCard
      }
      count
    }
  }
}

fragment CreaListingCard on CreaListing {
  ...CreaListingCore
  ...CreaListingAddress
  ...CreaListingBuilding
  ...CreaListingLargePhotos
  ...CreaListingAgentDetails
  ...CreaListingLocation
  ...CreaListingLand
}

fragment CreaListingCore on CreaListing {
  meta {
    id
  }
  listingId
  slug
  price
  transactionType
  propertyType
  ownershipType
  updatedAt
  createdAt
  property {
    slug
    location {
      lat
      lon
    }
    id
  }
}

fragment CreaListingAddress on CreaListing {
  address {
    streetAddress
    city
    province
    neighbourhood
    cityId
    neighbourhoodId
    postalCode
  }
}

fragment CreaListingBuilding on CreaListing {
  building {
    bedroomsTotal
    bathroomTotal
    type
    sizeInterior
    halfBathTotal
    constructedDate
    sizeInterior
    sizeExterior
    fireplacePresent
    basementDevelopment
    coolingType
    storiesTotal
  }
}

fragment CreaListingLargePhotos on CreaListing {
  photo {
    propertyPhoto {
      largePhotoUrl
      sequenceId
    }
  }
}

fragment CreaListingAgentDetails on CreaListing {
  agentDetails {
    name
    office {
      name
    }
  }
}

fragment CreaListingLocation on CreaListing {
  location {
    lat
    lon
  }
}

fragment CreaListingLand on CreaListing {
  land {
    sizeTotal
    sizeTotalText
  }
}

fragment PropertyCard on ESProperty {
  ...PropertyCore
  ...PropertyBuilding
  ...PropertyAddress
  ...PropertyLocation
  closeDate
  closePrice
  predictedDate
  predictedValue
}

fragment PropertyCore on ESProperty {
  id
  slug
  assessmentClass
  zoning
  accountNumber
  creaListing {
    meta {
      id
    }
    slug
    price
    location {
      lon
      lat
    }
  }
}

fragment PropertyBuilding on ESProperty {
  bathroomsTotal
  bedroomsTotal
  livingArea
  lotSizeArea
  yearBuiltActual
  show
  fireplace
  garageSpaces
  houseStyle
  livingAreaUnits
  basement
  building {
    propertyCount
  }
}

fragment PropertyAddress on ESProperty {
  unparsedAddress
  province
  city
  cityName
  neighbourhood
  neighbourhoodName
  postal
}

fragment PropertyLocation on ESProperty {
  location {
    lat
    lon
  }
}
"""

FILTERMAPDRAWER_VARIABLES = {
    "activeLayers": ["LISTINGS"],
    "creaListingsInput": {
        "bbox": None,
        "beds": {"gte": 0, "lte": None},
        "baths": {"gte": 0, "lte": None},
        "price": {"gte": None, "lte": None},
        "sort": {"field": "createdAt", "order": "DESC"},
        "homeType": [],
    },
}

CREALISTINGFULL_QUERY = """
query creaListingFull($slug: String, $id: String) {
  creaListing(slug: $slug, id: $id) {
    ...CreaListingFull
  }
}

fragment CreaListingFull on CreaListing {
  ...CreaListingCore
  ...CreaListingExtras
  ...CreaListingAddress
  ...CreaListingBuilding
  ...CreaListingBuildingRooms
  ...CreaListingLargePhotos
  ...CreaListingAgentDetails
  ...CreaListingAgentDetailsPhones
  ...CreaListingLocation
  ...CreaListingLand
}

fragment CreaListingCore on CreaListing {
  meta {
    id
  }
  listingId
  slug
  price
  transactionType
  propertyType
  ownershipType
  updatedAt
  createdAt
  property {
    slug
    location {
      lat
      lon
    }
    id
  }
}

fragment CreaListingExtras on CreaListing {
  parkingSpaceTotal
  viewType
  structure
  listingContractDate
  publicRemarks
  moreInformationLink
}

fragment CreaListingAddress on CreaListing {
  address {
    streetAddress
    city
    province
    neighbourhood
    cityId
    neighbourhoodId
    postalCode
  }
}

fragment CreaListingBuilding on CreaListing {
  building {
    bedroomsTotal
    bathroomTotal
    type
    sizeInterior
    halfBathTotal
    constructedDate
    sizeInterior
    sizeExterior
    fireplacePresent
    basementDevelopment
    coolingType
    storiesTotal
  }
}

fragment CreaListingBuildingRooms on CreaListing {
  building {
    rooms {
      room {
        level
        width
        length
        type
        dimension
      }
    }
  }
}

fragment CreaListingLargePhotos on CreaListing {
  photo {
    propertyPhoto {
      largePhotoUrl
      sequenceId
    }
  }
}

fragment CreaListingAgentDetails on CreaListing {
  agentDetails {
    name
    office {
      name
    }
  }
}

fragment CreaListingAgentDetailsPhones on CreaListing {
  agentDetails {
    phones {
      phone {
        meta {
          phoneType
        }
        value
      }
    }
  }
}

fragment CreaListingLocation on CreaListing {
  location {
    lat
    lon
  }
}

fragment CreaListingLand on CreaListing {
  land {
    sizeTotal
    sizeTotalText
  }
}
"""

PROPERTYFULL_QUERY = """
query propertyFull($slug: String, $id: String) {
  property(slug: $slug, id: $id) {
    ...PropertyFull
  }
}

fragment PropertyFull on ESProperty {
  ...PropertyCore
  ...PropertyBuilding
  ...PropertyBuildingEstimates
  ...PropertyPricesDates
  ...PropertyAddress
  ...PropertyLocation
  ...PropertyValuations
  ...PropertyAssessments
  ...PropertyRentalActual
  ...PropertyAirBnBActual
  ...PropertyCloses
}

fragment PropertyCore on ESProperty {
  id
  slug
  assessmentClass
  zoning
  accountNumber
  creaListing {
    meta {
      id
    }
    slug
    price
    location {
      lon
      lat
    }
  }
}

fragment PropertyBuilding on ESProperty {
  bathroomsTotal
  bedroomsTotal
  livingArea
  lotSizeArea
  yearBuiltActual
  show
  fireplace
  garageSpaces
  houseStyle
  livingAreaUnits
  basement
  building {
    propertyCount
  }
}

fragment PropertyBuildingEstimates on ESProperty {
  bathroomsTotalEst
  bedroomsTotalEst
  livingAreaEst
  lotSizeAreaEst
}

fragment PropertyPricesDates on ESProperty {
  closeDate
  closePrice
  lastEstimatedPrice
  lastEstimatedYear
  predictedDate
  predictedValue
}

fragment PropertyAddress on ESProperty {
  unparsedAddress
  province
  city
  cityName
  neighbourhood
  neighbourhoodName
  postal
}

fragment PropertyLocation on ESProperty {
  location {
    lat
    lon
  }
}

fragment PropertyValuations on ESProperty {
  valuations {
    id
    predictedValue
    predictedDate
  }
}

fragment PropertyAssessments on ESProperty {
  assessments {
    id
    value
    year
  }
}

fragment PropertyRentalActual on ESProperty {
  rentalActual {
    rentEstimate
    rentalYield
  }
}

fragment PropertyAirBnBActual on ESProperty {
  airbnbActual {
    airbnbEstimate
  }
}

fragment PropertyCloses on ESProperty {
  closes {
    id
    price
    date
  }
}
"""
