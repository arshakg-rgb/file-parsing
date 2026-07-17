# Datalead Project Structure Documentation

## Table of Contents

- [Overview](#overview)
- [Project Root Structure](#project-root-structure)
- [Source Code Structure](#source-code-structure-src)
- [API Controllers Structure](#api-controllers-structure-srcapi)
- [Service Layer Structure](#service-layer-structure-srcservice)
- [Utility Functions](#utility-functions-srcutils)
- [Configuration](#configuration-srcconfig)
- [Middleware](#middleware-srcmiddleware)
- [Routes](#routes-srcroutes)
- [Enums](#enums-srcenum)
- [Common](#common-srccommon)
- [Errors](#errors-srcerrors)
- [Loaders](#loaders-loaders)
- [Database Models](#database-models-srcconfigdbmodels)
- [Interface Definitions](#interface-definitions-io-folders)
- [Resource Classes](#resource-classes-resources)
- [Validation Pattern](#validation-pattern)
- [Singleton Pattern](#singleton-pattern)
- [Constants Pattern](#constants-pattern)
- [Error Handling Pattern](#error-handling-pattern)
- [Environment Configuration](#environment-configuration)
- [Docker Configuration](#docker-configuration)
- [Cloud Build Configuration](#cloud-build-configuration)
- [Database Configuration](#database-configuration)
- [API Versioning](#api-versioning)
- [Authentication & Authorization](#authentication--authorization)
- [Caching Strategy](#caching-strategy)
- [Socket.io Integration](#socketio-integration)
- [Logging](#logging)
- [Cron Jobs](#cron-jobs)
- [Testing](#testing)
- [File Naming Conventions](#file-naming-conventions)
- [Code Organization Principles](#code-organization-principles)
- [Integration Patterns](#integration-patterns)
- [Security Considerations](#security-considerations)
- [Deployment Architecture](#deployment-architecture)
- [Key Dependencies](#key-dependencies)
- [Development Workflow](#development-workflow)
- [Common Patterns to Replicate](#common-patterns-to-replicate)
- [Important Notes](#important-notes)
- [File Creation Checklist](#file-creation-checklist)
- [Maintenance Guidelines](#maintenance-guidelines)

---

## Overview
This document provides a comprehensive guide to the Datalead project structure, including all file names, class creation patterns, utilities, constants, and architectural patterns. Use this as a reference to replicate or understand the project structure.

## Project Root Structure

```bash
Datalead/
├── .env                    # Environment variables (not committed)
├── .env.example            # Example environment variables
├── Dockerfile              # Docker configuration for production
├── Dockerfile.local        # Docker configuration for local development
├── docker-compose.yml      # Docker Compose configuration
├── package.json            # Node.js dependencies
├── tsconfig.json           # TypeScript configuration
├── jest.config.ts          # Jest testing configuration
├── .sequelizerc            # Sequelize ORM configuration
├── cloudbuild.prod.yaml    # Google Cloud Build for production
├── cloudbuild.stage.yaml   # Google Cloud Build for staging
├── config/                 # Configuration files
├── loaders/                # Application loaders and initialization
├── sequalize/              # Database migrations and seeders
├── src/                    # Source code
├── templates/              # Template files
└── uploads/                # User upload directory
```

## Source Code Structure (`src/`)

```bash
src/
├── api/                    # API controllers and resources
├── app.ts                  # Express application setup
├── main.ts                 # Application entry point
├── common/                 # Common utilities and constants
├── config/                 # Configuration management
├── configs/                # Runtime configuration files
├── cronJob/                # Scheduled tasks
├── enum/                   # TypeScript enums
├── errors/                 # Custom error classes
├── mail/                   # Email functionality
├── middleware/             # Express middleware
├── repository/             # Data repository layer (empty)
├── routes/                 # API route definitions
├── service/                # Business logic services
├── tests/                  # Test files
└── utils/                  # Utility functions
```

## API Controllers Structure (`src/api/`)

### Controller Pattern
Each API module follows this structure:

```bash
api/{moduleName}/v{version}/
├── controllers/
│   ├── {ModuleName}Controller.ts           # Interface definition
│   └── impl/
│       └── {ModuleName}ControllerImpl.ts   # Implementation
├── resources/
│   └── {ResourceName}Resource.ts            # Data transformation
└── validations/
    └── {ActionName}Validation.ts            # Request validation
```

### API Modules

#### 1. Account Lookup (`src/api/accountLookup/`)

**v1/**: Legacy account lookup API
- `controllers/search/SearchReadController.ts`
- `resources/search/LampyreGeneralResource.ts`
- `resources/search/LampyreTelegramResource.ts`
- `resources/search/OsintResource.ts`
- `validations/search/searchReadValidation.ts`

**v2/**: Current account lookup API
- `controllers/accountLookup/AccountLookupController.ts`
- `controllers/accountLookup/impl/AccountLookupControllerImpl.ts`
- `controllers/httpTask/HttpTaskController.ts`
- `controllers/httpTask/impl/HttpTaskControllerImpl.ts`
- `controllers/image/ImageControllerI.ts`
- `controllers/image/impl/ImageControllerImpl.ts`
- `resources/request/HlrLookupResource.ts`
- `resources/request/LampyreGeneralResource.ts`
- `resources/request/LampyreTelegramResource.ts`
- `resources/request/LeakOsintResource.ts`
- `resources/request/OfflineDbResource.ts`
- `resources/request/OsintResource.ts`
- `resources/request/SyncMeResource.ts`
- `resources/request/WhatsappResource.ts`
- `validations/request/requestWriteValidation.ts`

#### 2. AI Report (`src/api/aiReport/`)
- `controllers/AiReportController.ts`
- `controllers/impl/AiReportControllerImpl.ts`
- `validations/request/requestGeneralValidation.ts`

#### 3. Anonymization (`src/api/anonymization/`)
- `controllers/AnonymizationController.ts`
- `controllers/impl/AnonymizationControllerImpl.ts`
- `validations/request/requestGeneralValidation.ts`

#### 4. Archive Lookup (`src/api/archiveLookup/`)
- `controllers/ArchiveLookupController.ts`
- `controllers/impl/ArchiveLookupControllerImpl.ts`
- `validations/request/requestGeneralValidation.ts`

#### 5. Dark Web Lookup (`src/api/darkWebLookup/`)
- `controllers/DarkwebLookupController.ts`
- `controllers/impl/DarkwebLookupControllerImpl.ts`
- `validations/request/requestGeneralValidation.ts`

#### 6. DB Sources (`src/api/dbSources/`)
- `controllers/DbSourceController.ts`
- `controllers/impl/DbSourceControllerImpl.ts`
- `validations/request/requestGeneralValidation.ts`

#### 7. Face Check (`src/api/facecheck/`)
- `controllers/FaceCheckController.ts`
- `controllers/impl/FaceCheckControllerImpl.ts`

#### 8. Face On Live (`src/api/faceonLive/`)
- `controllers/FaceOnLiveController.ts`
- `controllers/impl/FaceOnLiveControllerImpl.ts`
- `validations/request/requestGeneralValidation.ts`

#### 9. Notifications (`src/api/notifications/`)
- `controllers/NotificationController.ts`
- `controllers/impl/NotificationControllerImpl.ts`
- `validations/request/requestChangeStatusValidation.ts`

#### 10. PimEyes (`src/api/pimeyes/`)
- `controllers/PimEyesController.ts`
- `controllers/impl/PimEyesControllerImpl.ts`

#### 11. Premium Sources (`src/api/premiumSources/`)
- `controllers/PremiumSourcesController.ts`
- `controllers/impl/PremiumSourcesControllerImpl.ts`
- `resources/request/PremiumSourcesResource.ts`
- `validations/request/requestGeneralValidation.ts`

#### 12. Social Links (`src/api/socialLinks/`)
- `controllers/SocialLinksController.ts`
- `controllers/impl/SocialLinksControllerImpl.ts`
- `validations/socialLinksValidation.ts`

#### 13. Social Lookup (`src/api/socialLookup/`)

**v1/**: Legacy social lookup
- `controllers/request/RequestGeneralController.ts`
- `validations/request/requestGeneralValidation.ts`
- `validations/request/requestGeneralValidationV2.ts`
- `validations/request/requestGeneralValidationV3.ts`

**v2/**: Current social lookup
- `controllers/SocialLookupController.ts`
- `controllers/impl/SocialLookupControllerImpl.ts`

#### 14. Website (`src/api/website/`)
- `controllers/auth/AuthenticationController.ts`
- `controllers/announcement/AnnouncementController.ts`
- `controllers/announcement/impl/AnnouncementControllerImpl.ts`
- `controllers/apiClientLog/ApiClientLogController.ts`
- `controllers/apiClientLog/impl/ApiClientLogControllerImpl.ts`
- `controllers/apiLog/ApiLogController.ts`
- `controllers/apiLog/impl/ApiLogControllerImpl.ts`
- `controllers/apiType/ApiTypeController.ts`
- `controllers/documentation/DocumentationController.ts`
- `controllers/documentation/impl/DocumentationControllerImpl.ts`
- `controllers/credit/CreditController.ts`
- `controllers/credit/impl/CreditControllerImpl.ts`
- `controllers/generalSearchType/GeneralSearchTypeController.ts`
- `controllers/user/UserController.ts`
- `resources/announcement/AnnouncementResource.ts`
- `resources/apiClientLog/ApiClientLogIndexResource.ts`
- `resources/apiLog/ApiLogIndexResource.ts`
- `resources/apiSearchType/ApiSearchTypeIndexResource.ts`
- `resources/apiSearchType/io/ApiSearchTypeIndexResourceIo.ts`
- `resources/apiType/ApiTypeIndexResource.ts`
- `resources/apiType/ApiTypeShowResource.ts`
- `resources/creditBalance/CreditBalanceIndexResource.ts`
- `resources/docSource/DocSourceIndexResource.ts`
- `resources/generalSearchType/GeneralSearchTypeIndexResource.ts`
- `resources/user/AuthUserResource.ts`
- `resources/user/UserIndexResource.ts`
- `validations/auth/authenticationValidation.ts`
- `validations/auth/forgotPasswordValidation.ts`
- `validations/client/clientWriteValidation.ts`
- `validations/credit/creditWriteValidation.ts`

## Service Layer Structure (`src/service/`)

### Service Pattern
Each service module follows this structure:

```bash
service/{moduleName}/
├── {ModuleName}Service.ts              # Service interface
├── impl/
│   └── {ModuleName}ServiceImpl.ts      # Service implementation
└── io/
    ├── I{ModuleName}.ts                # Interface definitions
    └── {Constants}.ts                  # Constants and mappings
```

### Service Modules

#### 1. Account Lookup (`src/service/accountLookup/`)

**v1/**: Legacy account lookup
- `apiIntegration/ApiIntegrationService.ts`
- `apiIntegration/LampyreIntegrationService.ts`
- `apiIntegration/OsintIntegrationService.ts`

**v2/**: Current account lookup
- `accountLookup/AccountLookupService.ts`
- `accountLookup/ServiceFieldValidator.ts`
- `accountLookup/impl/AccountLookupServiceImpl.ts`
- `accountLookup/io/IAccountLookup.ts`
- `apiIntegration/Hlr_LookupIntegrationService.ts`
- `apiIntegration/LampyreEmailIntegrationService.ts`
- `apiIntegration/LampyreIntegrationService.ts`
- `apiIntegration/LampyrePhoneIntegrationService.ts`
- `apiIntegration/LeakOsintIntegrationService.ts`
- `apiIntegration/OfflineDbIntegrationService.ts`
- `apiIntegration/OsintIntegrationService.ts`
- `apiIntegration/SyncMeIntegrationService.ts`
- `apiIntegration/WhatsappIntegrationService.ts`
- `apiIntegration/impl/Hlr_LookupIntegrationServiceImpl.ts`
- `apiIntegration/impl/LampyreEmailIntegrationServiceImpl.ts`
- `apiIntegration/impl/LampyreIntegrationServiceImpl.ts`
- `apiIntegration/impl/LampyrePhoneIntegrationServiceImpl.ts`
- `apiIntegration/impl/LeakOsintIntegrationServiceImpl.ts`
- `apiIntegration/impl/OfflineDbIntegrationServiceImpl.ts`
- `apiIntegration/impl/OsintIntegrationServiceImpl.ts`
- `apiIntegration/impl/SyncMeIntegrationServiceImpl.ts`
- `apiIntegration/impl/WhatsappIntegrationServiceImpl.ts`
- `httpService/HttpTaskService.ts`
- `httpService/impl/HttpTaskServiceImpl.ts`

#### 2. AI Report (`src/service/aiReport/`)
- `v1/AiReportService.ts`
- `v1/impl/AiReportServiceImpl.ts`
- `v1/io/IAiReport.ts`

#### 3. Anonymization (`src/service/anonymization/`)
- `v1/AnonymizationService.ts`
- `v1/CloudTaskService.ts`
- `v1/impl/AnonymizationServiceImpl.ts`

#### 4. Archive Lookup (`src/service/archiveLookup/`)
- `ApiIntegrationService.ts`
- `v1/ApiIntegrationServiceImpl.ts`
- `v1/lolarchiver/DatabaseLookupService.ts`
- `v1/lolarchiver/KickService.ts`
- `v1/lolarchiver/TwitchService.ts`
- `v1/lolarchiver/TwitterHistoryLookupService.ts`
- `v1/lolarchiver/impl/DatabaseLookupServiceImpl.ts`
- `v1/lolarchiver/impl/KickServiceImpl.ts`
- `v1/lolarchiver/impl/TwitchServiceImpl.ts`
- `v1/lolarchiver/impl/TwitterHistoryLookupService.ts`
- `io/ISocialLookup.ts`

#### 5. Dark Web Lookup (`src/service/darkWebLookup/`)
- `v1/DarkOwlIntegrationService.ts`
- `v1/DarkwebApiIntegrationService.ts`
- `v1/DehashedIntegrationService.ts`
- `v1/impl/DarkOwlIntegrationServiceImpl.ts`
- `v1/impl/DarkwebApiIntegrationServiceImpl.ts`
- `v1/impl/DehashedIntegrationServiceImpl.ts`
- `v1/io/DarkOwlConstants.ts`
- `v1/io/IDarkOw.ts`
- `v1/io/IDarkwebLookup.ts`
- `v1/io/IDehashed.ts`

#### 6. DB Sources (`src/service/dbSources/`)
- `v1/DbSourceApiIntegrationService.ts`
- `v1/impl/DbSourceApiIntegrationServiceImpl.ts`
- `v1/io/IDbSource.ts`

#### 7. Face Check (`src/service/facecheck/`)
- `v1/FaceCheckService.ts`
- `v1/impl/FaceCheckServiceImpl.ts`

#### 8. Face On Live (`src/service/faceonLive/`)
- `v1/FaceOnLiveService.ts`
- `v1/impl/FaceOnLiveServiceImpl.ts`

#### 9. Image Search (`src/service/imageSearch/`)
- `v1/ImageSearchService.ts`
- `v1/impl/ImageSearchServiceImpl.ts`

#### 10. Notifications (`src/service/notifications/`)
- `v1/NotificationService.ts`
- `v1/impl/NotificationServiceImpl.ts`

#### 11. PimEyes (`src/service/pimeyes/`)
- `v1/PimEyesService.ts`
- `v1/impl/PimEyesServiceImpl.ts`

#### 12. Premium Sources (`src/service/premiumSources/`)
- `v1/PremiumSourcesService.ts`
- `v1/impl/PremiumSourcesServiceImpl.ts`

#### 13. Social Links (`src/service/socialLinks/`)
- `v1/SocialLinksService.ts`
- `v1/impl/SocialLinksServiceImpl.ts`
- `v1/io/ISocialLinks.ts`

#### 14. Social Media (`src/service/socialMedia/`)
- Multiple social media integration services

#### 15. Website (`src/service/website/`)
- `auth/AuthService.ts`
- `auth/impl/AuthServiceImpl.ts`
- `credit/CreditService.ts`
- `credit/impl/CreditServiceImpl.ts`
- `documentation/DocumentationService.ts`
- `documentation/impl/DocumentationServiceImpl.ts`

#### 16. General Services (`src/service/general/`)
- `HelperService.ts`
- `PermissionService.ts`
- `apiClientLog.ts`
- `apiLogDb.ts`

#### 17. API Type (`src/service/apiType/`)
- `ApiTypeService.ts`
- `impl/ApiTypeServiceImpl.ts`
- `io/ApiTypeIo.ts`

#### 18. General Search Type (`src/service/generalSearchType/`)
- `GeneralSearchTypeService.ts`
- `impl/GeneralSearchTypeServiceImpl.ts`

## Utility Functions (`src/utils/`)

```bash
utils/
├── cache/
│   ├── FirestoreCacheUtils.ts      # Firestore caching operations
│   └── RedisCacheUtils.ts          # Redis caching operations
├── formatter/
│   └── Formatter.ts                # Data formatting utilities
├── image/
│   └── ImageUtils.ts               # Image processing utilities
├── logger/
│   └── Log.ts                      # Logging configuration
├── luxon/
│   ├── DateUtils.ts                # Date/time utilities using Luxon
│   └── TimezoneUtils.ts            # Timezone handling
├── normalizers/
│   └── Normalizer.ts               # Data normalization
├── osint/
│   ├── OsintHelper.ts              # OSINT-specific helpers
│   └── OsintUtils.ts               # OSINT utilities
├── response/
│   └── ResponseHelper.ts           # API response formatting
├── router/
│   └── RouterHelper.ts             # Routing utilities
├── transformer/
│   └── Transformer.ts             # Data transformation
└── validator/
    └── Validator.ts               # Validation utilities
```

## Configuration (`src/config/`)

```bash
config/
├── ServiceManager.ts               # Service initialization
├── apiVerision/
│   ├── VersionManager.ts           # API version management
│   └── io/
│       └── ApiVersionConfig.ts      # API version configuration
├── axios/
│   └── AxiosManager.ts             # HTTP client configuration
├── cors/
│   └── CorsUtils.ts                # CORS configuration
├── db/
│   ├── MySqlManager.ts             # MySQL database manager
│   └── models/                     # Sequelize models
│       ├── AiReport.ts
│       ├── Announcement.ts
│       ├── ApiClientLog.ts
│       ├── ApiLog.ts
│       ├── ApiSearchType.ts
│       ├── ApiType.ts
│       ├── ApiTypesBalance.ts
│       ├── Cache.ts
│       ├── Client.ts
│       ├── Credit.ts
│       ├── CreditBalance.ts
│       ├── DocSource.ts
│       ├── GeneralSearchType.ts
│       ├── Notification.ts
│       ├── PersonalToken.ts
│       ├── RequestJob.ts
│       └── User.ts
├── firestore/
│   └── FirestoreManager.ts         # Firestore configuration
├── messaging/
│   └── socket/
│       ├── SocketEventManager.ts   # Socket event management
│       └── SocketManager.ts        # Socket.io configuration
├── osint/
│   ├── OsintProviders.ts           # OSINT provider configurations
│   ├── UnifiedOSINTService.ts     # Unified OSINT service
│   ├── io/
│   │   ├── IOsint.ts               # OSINT interfaces
│   │   └── OsintParamBuilderEnum.ts # OSINT parameter builders
│   └── validator/
│       └── OsintValidation.ts      # OSINT validation
└── system-config/
    ├── Config.ts                   # System configuration
    ├── ConfigValidator.ts          # Configuration validation
    └── io/
        ├── IAppConfig.ts           # App configuration interface
        ├── IAuthConfig.ts          # Auth configuration interface
        ├── ICommonConfig.ts        # Common configuration interface
        ├── IDatabaseConfig.ts      # Database configuration interface
        ├── IFirestoreConfig.ts     # Firestore configuration interface
        ├── IMessagingConfig.ts     # Messaging configuration interface
        ├── IOsintConfig.ts         # OSINT configuration interface
        └── IServicesConfig.ts      # Services configuration interface
```

## Middleware (`src/middleware/`)

```bash
middleware/
├── rateLimiterMiddleware.ts        # Rate limiting
├── requestGetJobMiddleware.ts       # Request job middleware
├── roleMiddleware.ts                # Role-based access control
├── searchCreditCheckMiddleware.ts   # Credit checking for searches
└── validationMiddleware.ts          # Request validation
```

## Routes (`src/routes/`)

```bash
routes/
├── accountLookup/                   # Account lookup routes
├── aiReport/                        # AI report routes
├── anonymization/                   # Anonymization routes
├── archiveLookup/                   # Archive lookup routes
├── darkWebLookup/                   # Dark web lookup routes
├── dbSources/                       # DB sources routes
├── facecheck/                       # Face check routes
├── faceonlive/                      # Face on live routes
├── image/                           # Image routes
├── index.ts                         # Main route file
├── jobs/                            # Job routes
├── landing/                         # Landing page routes
├── notifications/                   # Notification routes
├── pimeyes/                         # PimEyes routes
├── premiumSources/                  # Premium sources routes
├── socialLinks/                     # Social links routes
├── socialLookup/                    # Social lookup routes
├── stageTest/                       # Stage test routes
└── website/                         # Website routes
```

## Enums (`src/enum/`)

```bash
enum/
├── ApiSearchFieldEnum.ts            # API search field types
├── ArchiveLookupEnum.ts             # Archive lookup types
├── PersonalTokenTypeEnum.ts         # Personal token types
├── PhoneSearchFieldEnum.ts           # Phone search field types
├── RequestJobStatusEnum.ts          # Request job status
└── UserRoleEnum.ts                  # User role types
```

## Common (`src/common/`)

```bash
common/
├── enum/
│   ├── ApiTypeNameEnum.ts          # API type names
│   ├── GeneralSearchTypesEnum.ts    # General search types
│   └── StatusEnum.ts                # Status types
└── io/
    ├── Constants.ts                 # Common constants
    ├── IAuth.ts                     # Auth interface
    ├── ICustomService.ts            # Custom service interface
    ├── IPermission.ts               # Permission interface
    ├── IRequestJobResult.ts         # Request job result interface
    └── IServer.ts                   # Server interface
```

## Errors (`src/errors/`)

```bash
errors/
├── ApiError.ts                      # API error class
├── BadRequestError.ts              # Bad request error
├── CustomError.ts                   # Custom error base class
├── HttpError.ts                     # HTTP error class
├── InstantiationError.ts           # Singleton instantiation error
├── NonRetryableError.ts             # Non-retryable error
├── ServerError.ts                   # Server error class
├── SocketError.ts                   # Socket error class
├── ValidationError.ts               # Validation error class
└── io/
    └── ErrorInfo.ts                 # Error information interface
```

## Loaders (`loaders/`)

```bash
loaders/
├── api/
│   ├── middleware/
│   │   ├── authMiddleware.ts        # Authentication middleware
│   │   ├── permissionMiddleware.ts  # Permission middleware
│   │   └── validation.ts            # Validation middleware
│   ├── resources/
│   │   ├── BaseResource.ts          # Base resource class
│   │   └── stageTest.ts             # Stage test resource
│   └── validations/
│       └── BaseValidator.ts          # Base validator class
├── cronJob/
│   └── general/
│       ├── cacheCleanup.ts          # Cache cleanup job
│       └── log.ts                   # Log cleanup job
└── server/
    ├── logger.ts                    # Logger initialization
    └── mail.ts                      # Mail initialization
```

## Database Models (`src/config/db/models/`)

### Sequelize Models
All models use Sequelize ORM and follow this pattern:
- Define attributes with TypeScript types
- Include associations/relationships
- Use enums for status fields

### Model List
1. **AiReport.ts** - AI report generation records
2. **Announcement.ts** - System announcements
3. **ApiClientLog.ts** - API client request logs
4. **ApiLog.ts** - API operation logs
5. **ApiSearchType.ts** - API search type definitions
6. **ApiType.ts** - API type configurations
7. **ApiTypesBalance.ts** - API type credit balances
8. **Cache.ts** - Cache records
9. **Client.ts** - Client information
10. **Credit.ts** - Credit transactions
11. **CreditBalance.ts** - User credit balances
12. **DocSource.ts** - Document source configurations
13. **GeneralSearchType.ts** - General search type definitions
14. **Notification.ts** - User notifications
15. **PersonalToken.ts** - Personal access tokens
16. **RequestJob.ts** - Request job records
17. **User.ts** - User accounts

## Interface Definitions (`io/` folders)

### Interface Pattern
Each service module has an `io/` folder containing:
- **I{ModuleName}.ts** - Main interface definitions
- **{Constants}.ts** - Constants and field mappings
- **I{Specific}.ts** - Specific operation interfaces

### Key Interfaces
- **IAccountLookup.ts** - Account lookup operations
- **IAiReport.ts** - AI report operations
- **IDarkOw.ts** - Dark Owl API interfaces
- **IDarkwebLookup.ts** - Dark web lookup operations
- **IDehashed.ts** - Dehashed API interfaces
- **ISocialLookup.ts** - Social lookup operations
- **IDbSource.ts** - DB source operations
- **IPermission.ts** - Permission definitions
- **IAuth.ts** - Authentication interfaces

## Resource Classes (`resources/`)

### Resource Pattern
Resources transform API responses into standardized formats:
- Extend `BaseResource.ts`
- Implement `toJSON()` method
- Map API fields to standard field names
- Add metadata (social_type, source, etc.)

### Base Resource (`loaders/api/resources/BaseResource.ts`)

```typescript
class BaseResource {
  toJSON() { /* transformation logic */ }
  static collection(data) { /* array transformation */ }
  paginate(data) { /* pagination */ }
}
```

## Validation Pattern

### Validator Pattern
Validators use `BaseValidator.ts`:
- Define validation rules
- Implement `validate()` method
- Return validation errors or success

### Validation Files
- `BaseValidator.ts` - Base validator class
- `{Action}Validation.ts` - Specific action validators

## Singleton Pattern

### Service Implementation Pattern
All services use singleton pattern:

```typescript
export class ServiceImpl implements Service {
  private static instance: ServiceImpl;

  constructor(enforce: () => void) {
    if (enforce !== Enforce) {
      throw new InstantiationError(...);
    }
  }

  public static getInstance(): Service {
    if (!ServiceImpl.instance) {
      ServiceImpl.instance = new ServiceImpl(Enforce);
    }
    return ServiceImpl.instance;
  }
}

function Enforce(): void {}
```

## Constants Pattern

### Constants Files
Constants files contain:
- Field mappings (e.g., `ip_address: 'ip'`)
- API endpoints
- Configuration values
- Enum mappings

Example (`DarkOwlConstants.ts`):
```typescript
export const GENERAL_SEARCH_FIELD_MAPPING = {
  ip_address: 'ip',
  crypto_address: 'crypto',
  user: 'username'
} as const;
```

## Error Handling Pattern

### Error Classes
All errors extend base classes:
- `CustomError` - Base error class
- `ApiError` - API-specific errors
- `HttpError` - HTTP-specific errors
- `ServerError` - Server errors
- `ValidationError` - Validation errors

### Error Usage

```typescript
throw new ValidationError(ValidationError.INPUT, "Invalid input");
throw new ApiError("API request failed", 500);
throw new ServerError(ServerError.INTERNAL, "Internal error");
```

## Environment Configuration

### Environment Variables
Key environment variables (`.env`):
- Database connection strings
- API tokens (OSINT, Lampyre, etc.)
- Firebase/Firestore configuration
- JWT secrets
- Cloud project IDs
- Service account credentials

### Configuration Files
- `config/services.ts` - External service configurations
- `config/system-config/Config.ts` - System configuration
- `.env.example` - Environment variable template

## Docker Configuration

### Dockerfile Structure
- Base image: `node:20.18`
- System dependencies for Chrome/Puppeteer
- Chrome headless shell installation
- Non-root user setup
- Build and deployment steps

### Docker Compose
- Service definitions
- Volume mounts
- Network configuration
- Environment variables

## Cloud Build Configuration

### Cloud Build YAML
- Build steps (install, build, test)
- Docker image build and push
- Cloud Run deployment
- Environment variable setup from secrets

## Database Configuration

### Sequelize Configuration
- MySQL connection via `MySqlManager.ts`
- Model definitions in `config/db/models/`
- Migrations in `sequalize/migrations/`
- Seeders in `sequalize/seeders/`

### Firestore Configuration
- Connection via `FirestoreManager.ts`
- Cache operations via `FirestoreCacheUtils.ts`

## API Versioning

### Version Structure
- Each API module has version folders (v1, v2)
- Route paths include version: `/api/{module}/v{version}/`
- Controller classes include version in name

### Version Management
- `ApiVersionConfig.ts` - Version configuration
- `VersionManager.ts` - Version management logic

## Authentication & Authorization

### Authentication
- JWT-based authentication
- Token validation in middleware
- User session management

### Authorization
- Role-based access control (`roleMiddleware.ts`)
- Permission checking (`PermissionService.ts`)
- API type permissions

## Caching Strategy

### Cache Implementation
- **Redis Cache** - `RedisCacheUtils.ts` (legacy)
- **Firestore Cache** - `FirestoreCacheUtils.ts` (current)
- Cache keys pattern: `request-{apiId}:{requestJobId}`
- Cache TTL configuration

## Socket.io Integration

### Socket Configuration
- `SocketManager.ts` - Socket.io server setup
- `SocketEventManager.ts` - Event management
- Real-time notifications for job updates

## Logging

### Logger Configuration
- Pino logger via `logger/Log.ts`
- Structured logging with context
- Log levels: info, warn, error, debug
- Log retention: 30 days

## Cron Jobs

### Scheduled Tasks
- Cache cleanup (`cacheCleanup.ts`)
- Log cleanup (`log.ts`)
- Configured in `cronJob/` directory

## Testing

### Test Configuration
- Jest configuration in `jest.config.ts`
- Test files in `src/tests/`
- Integration tests and unit tests

## File Naming Conventions

### Controllers
- `{ModuleName}Controller.ts` - Interface
- `{ModuleName}ControllerImpl.ts` - Implementation

### Services
- `{ModuleName}Service.ts` - Interface
- `{ModuleName}ServiceImpl.ts` - Implementation

### Resources
- `{ResourceName}Resource.ts` - Data transformation

### Validators
- `{Action}Validation.ts` - Validation rules

### Interfaces
- `I{ModuleName}.ts` - Interface definitions
- `I{Operation}.ts` - Operation-specific interfaces

### Constants
- `{ModuleName}Constants.ts` - Module constants
- `{Category}Enum.ts` - Enum definitions

## Code Organization Principles

### Separation of Concerns
- **Controllers** - Handle HTTP requests/responses
- **Services** - Business logic
- **Repositories** - Data access (currently minimal)
- **Resources** - Data transformation
- **Validators** - Input validation
- **Middleware** - Cross-cutting concerns

### Dependency Injection
- Singleton pattern for services
- Service instances obtained via `getInstance()`
- Constructor injection for dependencies

### Error Handling
- Custom error classes for different error types
- Centralized error handling in middleware
- Structured error logging

## Integration Patterns

### API Integration
- Each external API has dedicated integration service
- Integration services handle:
  - HTTP requests
  - Response transformation
  - Error handling
  - Caching
  - Rate limiting

### Resource Transformation
- API responses transformed via Resource classes
- Standardized field names
- Added metadata (social_type, source, etc.)
- Deduplication and filtering

## Security Considerations

### Authentication
- JWT token validation
- Role-based access control
- Permission checking per API type

### Rate Limiting
- `rateLimiterMiddleware.ts` for API rate limiting
- Credit-based rate limiting for searches
- Per-user and per-API limits

### Data Protection
- Environment variables for sensitive data
- Secret management via Google Cloud Secrets
- Non-root user in Docker

## Deployment Architecture

### Production Deployment
- Google Cloud Run for application hosting
- Cloud SQL for MySQL database
- Firestore for caching
- Cloud Build for CI/CD
- Artifact Registry for Docker images

### Environment Configuration
- Stage environment: `cloudbuild.stage.yaml`
- Production environment: `cloudbuild.prod.yaml`
- Environment-specific configurations

## Key Dependencies

### Core Dependencies
- Express.js - Web framework
- Sequelize - ORM
- TypeScript - Type safety
- Jest - Testing framework
- Pino - Logging
- Socket.io - Real-time communication
- Axios - HTTP client
- Luxon - Date/time handling

### External API Integrations
- OSINT Industries
- Lampyre
- Dehashed
- Dark Owl
- LolArchiver
- SocialCity
- And others...

## Development Workflow

### Local Development
1. Install dependencies: `npm install`
2. Configure environment: Copy `.env.example` to `.env`
3. Build: `npm run build`
4. Run: `npm start` or `npm run dev`

### Testing
- Unit tests: `npm run test`
- Smoke tests: `npm run smoke-test`
- Integration tests: `npm run test:integration`

### Building
- TypeScript compilation: `npm run build`
- Output directory: `dist/`

## Common Patterns to Replicate

### 1. Creating a New API Module
1. Create controller interface: `api/{module}/v1/controllers/{Module}Controller.ts`
2. Create controller implementation: `api/{module}/v1/controllers/impl/{Module}ControllerImpl.ts`
3. Create service interface: `service/{module}/{Module}Service.ts`
4. Create service implementation: `service/{module}/impl/{Module}ServiceImpl.ts`
5. Create interfaces: `service/{module}/io/I{Module}.ts`
6. Create routes: `routes/{module}/index.ts`
7. Add to main routes: `routes/index.ts`

### 2. Creating a New Service
1. Define interface in `io/` folder
2. Create service interface
3. Implement with singleton pattern
4. Add to `ServiceManager.ts` if needed
5. Configure in `config/services.ts`

### 3. Adding Database Model
1. Create model in `config/db/models/{ModelName}.ts`
2. Define attributes and associations
3. Create migration in `sequalize/migrations/`
4. Create seeder if needed in `sequalize/seeders/`

### 4. Adding External API Integration
1. Create integration service in `service/{module}/apiIntegration/`
2. Implement HTTP request handling
3. Add resource transformation
4. Configure API token in `config/services.ts`
5. Add to service integration logic

## Important Notes

### Singleton Pattern
- All services must use singleton pattern
- Always use `getInstance()` to obtain service instance
- Never instantiate services directly with `new`

### Error Handling
- Always use custom error classes
- Include proper error messages and status codes
- Log errors with context

### Caching
- Use FirestoreCacheUtils for new implementations
- Follow cache key pattern: `request-{apiId}:{requestJobId}`
- Implement cache invalidation where needed

### API Versioning
- New features should use v2 or higher
- Maintain backward compatibility where possible
- Document breaking changes

### Security
- Never commit sensitive data
- Use environment variables for secrets
- Validate all user inputs
- Implement proper authentication/authorization

## File Creation Checklist

When creating new files, ensure:
- [ ] Follow naming conventions
- [ ] Implement proper interfaces
- [ ] Add error handling
- [ ] Include logging
- [ ] Add validation where needed
- [ ] Follow singleton pattern for services
- [ ] Add to appropriate route file
- [ ] Update documentation
- [ ] Add tests
- [ ] Configure environment variables if needed

## Maintenance Guidelines

### Code Quality
- Use TypeScript strict mode
- Follow existing code style
- Add JSDoc comments for public methods
- Keep functions focused and small

### Performance
- Implement caching for expensive operations
- Use pagination for large datasets
- Optimize database queries
- Implement rate limiting

### Monitoring
- Log all API calls
- Track performance metrics
- Monitor error rates
- Set up alerts for critical failures

This document provides a comprehensive guide to the Datalead project structure. Use it as a reference for understanding the architecture, creating new features, or replicating the structure in another project.
