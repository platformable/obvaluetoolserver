from typing import List, Union, Optional
from pydantic import BaseModel
from fastapi import FastAPI, Query
from sqlmodel import  SQLModel, create_engine, Field, Session, select, col, MetaData, Table, text
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import JSON
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.sql import func
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI()

origins = [
    "http://localhost:3000"
]

DATABASE_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")



app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


engine = create_engine(f"postgresql://{DB_USER}:{DATABASE_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?options=-csearch_path%3Dschemacoredataset")



class Stakeholders(SQLModel, table=True):
    __tablename__ = "Stakeholders"
    __table_args__ = {"schema": "schemacoredataset"}

    IdSegment: str = Field(primary_key=True)
    Segment: str

class Region(SQLModel, table=True):
    __tablename__ = "Region"
    __table_args__ = {"schema": "schemacoredataset"}

    IdRegion: str = Field(primary_key=True)
    RegionDetail: str

@app.get("/")
def read_root():
    with Session(engine) as session:
        query = select(Stakeholders)
        result = session.exec(query).all()
        print(result)
        return result

@app.get("/stakeholders")
def read_root():
    with Session(engine) as session:
        query = select(Stakeholders).order_by(Stakeholders.Segment)
        result = session.exec(query).all()
        return result
    
@app.get("/regions")
def read_root():
    with Session(engine) as session:
        query = select(Region).where(Region.RegionDetail != "Russia & Belarus").order_by(Region.RegionDetail)
        result = session.exec(query).all()
        return result


metadata = MetaData(schema="schemacoredataset")

ValueGeneratedTool = Table(
    "ValueGeneratedTool",
    metadata,
    autoload_with=engine
)

ValueEntities = Table(
    "nc_18z6___nc_m2m_jvbe0yrzlx",
    metadata,
    autoload_with=engine
)

Entities = Table(
    "Entities",
    metadata,
    autoload_with=engine
)

ValueStakeholders = Table(
    "nc_18z6___nc_m2m_92jecqh6hi",
    metadata,
    autoload_with=engine
)

ValueTaxonomyConn = Table(
    "nc_18z6___nc_m2m_7g_er1fyle",
    metadata,
    autoload_with=engine
)

ValueCountryConn = Table(
    "nc_18z6___nc_m2m_2ddjpb613i",
    metadata,
    autoload_with=engine
)

Country = Table(
    "Country",
    metadata,
    autoload_with=engine
)

StakeholdersTable = Table(
    "Stakeholders",
    metadata,
    autoload_with=engine
)

RegionTable = Table(
    "Region",
    metadata,
    autoload_with=engine
)


@app.get("/value-generated")
# def read_value_generated(paginationCounter: int = 0, limit: int = 30, value: str = None, region: str = None, stakeholder: str = None):
def read_value_generated(paginationCounter: int = 0, limit: int = 30,
                         values: Optional[List[str]] = Query(None),
                         regions: Optional[List[str]] = Query(None),
                         stakeholders: Optional[List[str]] = Query(None)):
    with Session(engine) as session:
        # calculate pagination
        offset = paginationCounter * limit
        page = paginationCounter + 1

        
        base_no_filter = (
            select(
                ValueGeneratedTool.c.DataPoint,
                ValueGeneratedTool.c.DataPointNarrative,
                ValueGeneratedTool.c.DownloadLink,
                ValueGeneratedTool.c.SourceLink,
                ValueTaxonomy.c.ValueGenerationCategory,
                Entities.c.EntityLogo,
                StakeholdersTable.c.Segment,
                RegionTable.c.Region
            )
            .select_from(ValueGeneratedTool)
            .join(ValueEntities, ValueEntities.c.table2_id == ValueGeneratedTool.c.id, isouter=True)
            .join(Entities, Entities.c.IdEntity == ValueEntities.c.table1_id, isouter=True)
            .join(ValueStakeholders, ValueGeneratedTool.c.id == ValueStakeholders.c.table2_id, isouter=True)
            .join(StakeholdersTable, ValueStakeholders.c.table1_id == StakeholdersTable.c.IdSegment, isouter=True)
            .join(ValueTaxonomyConn, ValueGeneratedTool.c.id == ValueTaxonomyConn.c.table2_id, isouter=True)
            .join(ValueTaxonomy, ValueTaxonomy.c.IdValueTaxonomy == ValueTaxonomyConn.c.table1_id, isouter=True)
            .join(ValueCountryConn, ValueGeneratedTool.c.id == ValueCountryConn.c.table2_id, isouter=True)
            .join(Country, ValueCountryConn.c.table1_id == Country.c.IdCountry, isouter=True)
            .join(RegionTable, Country.c.Region_id == RegionTable.c.IdRegion, isouter=True)
        )

        
        def _expand_param(param: Optional[List[str]]) -> List[str]:
            out: List[str] = []
            if not param:
                return out
            for p in param:
                parts = [s.strip() for s in p.split(",") if s.strip()]
                out.extend(parts)
            return out

        
        base_with_filters = base_no_filter
        filters_applied = False
        values_list = _expand_param(values)
        regions_list = _expand_param(regions)
        stakeholders_list = _expand_param(stakeholders)

        if values_list:
            base_with_filters = base_with_filters.where(ValueTaxonomy.c.ValueGenerationCategory.in_(values_list))
            filters_applied = True
        if regions_list:
            base_with_filters = base_with_filters.where(RegionTable.c.RegionDetail.in_(regions_list))
            filters_applied = True
        if stakeholders_list:
            base_with_filters = base_with_filters.where(StakeholdersTable.c.Segment.in_(stakeholders_list))
            filters_applied = True

        
        where_clauses = []
        if values_list:
            where_clauses.append(ValueTaxonomy.c.ValueGenerationCategory.in_(values_list))
        if regions_list:
            where_clauses.append(RegionTable.c.RegionDetail.in_(regions_list))
        if stakeholders_list:
            where_clauses.append(StakeholdersTable.c.Segment.in_(stakeholders_list))

        
        id_subq = (
            select(ValueGeneratedTool.c.id)
            .select_from(ValueGeneratedTool)
            .join(ValueEntities, ValueEntities.c.table2_id == ValueGeneratedTool.c.id, isouter=True)
            .join(Entities, Entities.c.IdEntity == ValueEntities.c.table1_id, isouter=True)
            .join(ValueStakeholders, ValueGeneratedTool.c.id == ValueStakeholders.c.table2_id, isouter=True)
            .join(StakeholdersTable, ValueStakeholders.c.table1_id == StakeholdersTable.c.IdSegment, isouter=True)
            .join(ValueTaxonomyConn, ValueGeneratedTool.c.id == ValueTaxonomyConn.c.table2_id, isouter=True)
            .join(ValueTaxonomy, ValueTaxonomy.c.IdValueTaxonomy == ValueTaxonomyConn.c.table1_id, isouter=True)
            .join(ValueCountryConn, ValueGeneratedTool.c.id == ValueCountryConn.c.table2_id, isouter=True)
            .join(Country, ValueCountryConn.c.table1_id == Country.c.IdCountry, isouter=True)
            .join(RegionTable, Country.c.Region_id == RegionTable.c.IdRegion, isouter=True)
            .where(*where_clauses)
            .distinct()
            .subquery()
        )

        count_query = select(func.count()).select_from(id_subq)
        total_rows = session.exec(count_query).one()

        
        agg_query = (
            select(
                ValueGeneratedTool.c.DataPoint,
                ValueGeneratedTool.c.DataPointNarrative,
                ValueGeneratedTool.c.DownloadLink,
                ValueGeneratedTool.c.SourceLink,
                func.string_agg(func.distinct(ValueTaxonomy.c.ValueGenerationCategory), ", ").label("ValueGenerationCategory"),
                func.string_agg(func.distinct(Entities.c.EntityLogo), ", ").label("EntityLogo"),
                func.string_agg(func.distinct(StakeholdersTable.c.Segment), ", ").label("Segment"),
                func.string_agg(func.distinct(RegionTable.c.RegionDetail), ", ").label("Region")
            )
            .select_from(ValueGeneratedTool)
            .join(ValueEntities, ValueEntities.c.table2_id == ValueGeneratedTool.c.id)
            .join(Entities, Entities.c.IdEntity == ValueEntities.c.table1_id)
            .join(ValueStakeholders, ValueGeneratedTool.c.id == ValueStakeholders.c.table2_id)
            .join(StakeholdersTable, ValueStakeholders.c.table1_id == StakeholdersTable.c.IdSegment)
            .join(ValueTaxonomyConn, ValueGeneratedTool.c.id == ValueTaxonomyConn.c.table2_id)
            .join(ValueTaxonomy, ValueTaxonomy.c.IdValueTaxonomy == ValueTaxonomyConn.c.table1_id)
            .join(ValueCountryConn, ValueGeneratedTool.c.id == ValueCountryConn.c.table2_id)
            .join(Country, ValueCountryConn.c.table1_id == Country.c.IdCountry)
            .join(RegionTable, Country.c.Region_id == RegionTable.c.IdRegion)
            .where(*where_clauses)
            .group_by(
                ValueGeneratedTool.c.id,
                ValueGeneratedTool.c.DataPoint,
                ValueGeneratedTool.c.DataPointNarrative,
                ValueGeneratedTool.c.DownloadLink,
                ValueGeneratedTool.c.SourceLink,
            )
            .offset(offset)
            .limit(limit)
        )

        result = session.exec(agg_query).mappings().all()
        is_first_page = paginationCounter == 0
        # total_rows is a tuple-like ScalarResult for select(count()).one(), extract int
        total_rows_int = int(total_rows[0]) if isinstance(total_rows, tuple) or hasattr(total_rows, "__iter__") else int(total_rows)
        is_last_page = offset + len(result) >= total_rows_int

        return {
            "data": result,
            "pageInfo": {
                "isFirstPage": is_first_page,
                "isLastPage": is_last_page,
                "page": page,
                "pageSize": limit,
                "totalRows": total_rows_int
            }
        }





ValueTaxonomy = Table(
    "ValueTaxonomy",
    metadata,
    autoload_with=engine
)

TaxonomyValueClusters = Table(
    "nc_j64s___nc_m2m_vta36xadlc",
    metadata,
    autoload_with=engine
)

ValueClusters = Table(
    "ValueClusters",
    metadata,
    autoload_with=engine
)

@app.get("/value-taxonomy")
def read_value_taxonomy():
    """Return ValueGenerationCategory and its ValueCluster ordered by ValueCluster ascending."""
    with Session(engine) as session:
        query = (
            select(
                ValueTaxonomy.c.ValueGenerationCategory,
                ValueClusters.c.ValueCluster.label("ClusterList")
            )
            .join(
                TaxonomyValueClusters,
                ValueTaxonomy.c.IdValueTaxonomy == TaxonomyValueClusters.c.table1_id
            )
            .join(
                ValueClusters,
                TaxonomyValueClusters.c.table2_id == ValueClusters.c.IdClusterCategory
            )
            .order_by(ValueClusters.c.ValueCluster.asc())
        )

        result = session.exec(query).mappings().all()
        return result
    

@app.get("/test")
def read_value_taxonomy_clusters(region: str = "APAC"):
   return region




class ValueToolResponse(BaseModel):
    DataPoint: str
    DataPointNarrative: Optional[str]
    DownloadLink: Optional[str]
    SourceLink: Optional[str]
    Categories: Optional[str]
    Logos: Optional[str]
    Segments: Optional[str]
    Regions: Optional[str]

    """ class Config:
        from_attributes = True """


@app.get("/value-tools", response_model=List[ValueToolResponse])
def get_value_tools():

     with Session(engine) as session:
        query = text("""
        SELECT 
            value."DataPoint", 
            value."DataPointNarrative", 
            value."DownloadLink", 
            value."SourceLink",   
            STRING_AGG(DISTINCT valuetaxonomy."ValueGenerationCategory", ', ') AS "Categories",
            STRING_AGG(DISTINCT entities."EntityLogo", ', ') AS "Logos", 
            STRING_AGG(DISTINCT stakeholders."Segment", ', ') AS "Segments",  
            STRING_AGG(DISTINCT region."Region", ', ') AS "Regions"
        FROM schemacoredataset."ValueGeneratedTool" value
        LEFT JOIN schemacoredataset."nc_18z6___nc_m2m_jvbe0yrzlx" value_entities ON value_entities.table2_id = value.id
        LEFT JOIN schemacoredataset."Entities" entities ON entities."IdEntity" = value_entities.table1_id
        LEFT JOIN schemacoredataset."nc_18z6___nc_m2m_92jecqh6hi" value_stakeholders ON value.id = value_stakeholders.table2_id
        LEFT JOIN schemacoredataset."Stakeholders" stakeholders ON value_stakeholders.table1_id = stakeholders."IdSegment" 
        LEFT JOIN schemacoredataset."nc_18z6___nc_m2m_7g_er1fyle" value_taxonomy_conn ON value.id = value_taxonomy_conn.table2_id
        LEFT JOIN schemacoredataset."ValueTaxonomy" valuetaxonomy ON valuetaxonomy."IdValueTaxonomy" = value_taxonomy_conn.table1_id
        LEFT JOIN schemacoredataset."nc_18z6___nc_m2m_2ddjpb613i" value_country_conn ON value.id = value_country_conn.table2_id
        LEFT JOIN schemacoredataset."Country" country ON value_country_conn.table1_id = country."IdCountry"
        LEFT JOIN schemacoredataset."Region" region ON country."Region_id" = region."IdRegion"
        GROUP BY 
            value.id, 
            value."DataPoint", 
            value."DataPointNarrative", 
            value."DownloadLink", 
            value."SourceLink";
    """)
    
        result = session.exec(query).mappings().all()
        return result