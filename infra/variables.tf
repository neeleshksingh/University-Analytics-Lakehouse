variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "centralindia"
}

variable "project" {
  description = "Short project name used in resource names"
  type        = string
  default     = "uni"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}