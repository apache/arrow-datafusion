impl serde::Serialize for AggLimit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.limit != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggLimit", len)?;
        if self.limit != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("limit", ToString::to_string(&self.limit).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggLimit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "limit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Limit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "limit" => Ok(GeneratedField::Limit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggLimit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggLimit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggLimit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut limit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(AggLimit {
                    limit: limit__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggLimit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.group_expr.is_empty() {
            len += 1;
        }
        if !self.aggr_expr.is_empty() {
            len += 1;
        }
        if self.mode != 0 {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if !self.group_expr_name.is_empty() {
            len += 1;
        }
        if !self.aggr_expr_name.is_empty() {
            len += 1;
        }
        if self.input_schema.is_some() {
            len += 1;
        }
        if !self.null_expr.is_empty() {
            len += 1;
        }
        if !self.groups.is_empty() {
            len += 1;
        }
        if !self.filter_expr.is_empty() {
            len += 1;
        }
        if self.limit.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateExecNode", len)?;
        if !self.group_expr.is_empty() {
            struct_ser.serialize_field("groupExpr", &self.group_expr)?;
        }
        if !self.aggr_expr.is_empty() {
            struct_ser.serialize_field("aggrExpr", &self.aggr_expr)?;
        }
        if self.mode != 0 {
            let v = AggregateMode::try_from(self.mode)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.mode)))?;
            struct_ser.serialize_field("mode", &v)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.group_expr_name.is_empty() {
            struct_ser.serialize_field("groupExprName", &self.group_expr_name)?;
        }
        if !self.aggr_expr_name.is_empty() {
            struct_ser.serialize_field("aggrExprName", &self.aggr_expr_name)?;
        }
        if let Some(v) = self.input_schema.as_ref() {
            struct_ser.serialize_field("inputSchema", v)?;
        }
        if !self.null_expr.is_empty() {
            struct_ser.serialize_field("nullExpr", &self.null_expr)?;
        }
        if !self.groups.is_empty() {
            struct_ser.serialize_field("groups", &self.groups)?;
        }
        if !self.filter_expr.is_empty() {
            struct_ser.serialize_field("filterExpr", &self.filter_expr)?;
        }
        if let Some(v) = self.limit.as_ref() {
            struct_ser.serialize_field("limit", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "group_expr",
            "groupExpr",
            "aggr_expr",
            "aggrExpr",
            "mode",
            "input",
            "group_expr_name",
            "groupExprName",
            "aggr_expr_name",
            "aggrExprName",
            "input_schema",
            "inputSchema",
            "null_expr",
            "nullExpr",
            "groups",
            "filter_expr",
            "filterExpr",
            "limit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            GroupExpr,
            AggrExpr,
            Mode,
            Input,
            GroupExprName,
            AggrExprName,
            InputSchema,
            NullExpr,
            Groups,
            FilterExpr,
            Limit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "groupExpr" | "group_expr" => Ok(GeneratedField::GroupExpr),
                            "aggrExpr" | "aggr_expr" => Ok(GeneratedField::AggrExpr),
                            "mode" => Ok(GeneratedField::Mode),
                            "input" => Ok(GeneratedField::Input),
                            "groupExprName" | "group_expr_name" => Ok(GeneratedField::GroupExprName),
                            "aggrExprName" | "aggr_expr_name" => Ok(GeneratedField::AggrExprName),
                            "inputSchema" | "input_schema" => Ok(GeneratedField::InputSchema),
                            "nullExpr" | "null_expr" => Ok(GeneratedField::NullExpr),
                            "groups" => Ok(GeneratedField::Groups),
                            "filterExpr" | "filter_expr" => Ok(GeneratedField::FilterExpr),
                            "limit" => Ok(GeneratedField::Limit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut group_expr__ = None;
                let mut aggr_expr__ = None;
                let mut mode__ = None;
                let mut input__ = None;
                let mut group_expr_name__ = None;
                let mut aggr_expr_name__ = None;
                let mut input_schema__ = None;
                let mut null_expr__ = None;
                let mut groups__ = None;
                let mut filter_expr__ = None;
                let mut limit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::GroupExpr => {
                            if group_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupExpr"));
                            }
                            group_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AggrExpr => {
                            if aggr_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrExpr"));
                            }
                            aggr_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Mode => {
                            if mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("mode"));
                            }
                            mode__ = Some(map_.next_value::<AggregateMode>()? as i32);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::GroupExprName => {
                            if group_expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupExprName"));
                            }
                            group_expr_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AggrExprName => {
                            if aggr_expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrExprName"));
                            }
                            aggr_expr_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::InputSchema => {
                            if input_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputSchema"));
                            }
                            input_schema__ = map_.next_value()?;
                        }
                        GeneratedField::NullExpr => {
                            if null_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullExpr"));
                            }
                            null_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Groups => {
                            if groups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groups"));
                            }
                            groups__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FilterExpr => {
                            if filter_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filterExpr"));
                            }
                            filter_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AggregateExecNode {
                    group_expr: group_expr__.unwrap_or_default(),
                    aggr_expr: aggr_expr__.unwrap_or_default(),
                    mode: mode__.unwrap_or_default(),
                    input: input__,
                    group_expr_name: group_expr_name__.unwrap_or_default(),
                    aggr_expr_name: aggr_expr_name__.unwrap_or_default(),
                    input_schema: input_schema__,
                    null_expr: null_expr__.unwrap_or_default(),
                    groups: groups__.unwrap_or_default(),
                    filter_expr: filter_expr__.unwrap_or_default(),
                    limit: limit__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.aggr_function != 0 {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.distinct {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        if !self.order_by.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateExprNode", len)?;
        if self.aggr_function != 0 {
            let v = AggregateFunction::try_from(self.aggr_function)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.aggr_function)))?;
            struct_ser.serialize_field("aggrFunction", &v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.distinct {
            struct_ser.serialize_field("distinct", &self.distinct)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        if !self.order_by.is_empty() {
            struct_ser.serialize_field("orderBy", &self.order_by)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "aggr_function",
            "aggrFunction",
            "expr",
            "distinct",
            "filter",
            "order_by",
            "orderBy",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AggrFunction,
            Expr,
            Distinct,
            Filter,
            OrderBy,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "expr" => Ok(GeneratedField::Expr),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "filter" => Ok(GeneratedField::Filter),
                            "orderBy" | "order_by" => Ok(GeneratedField::OrderBy),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut aggr_function__ = None;
                let mut expr__ = None;
                let mut distinct__ = None;
                let mut filter__ = None;
                let mut order_by__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::AggrFunction => {
                            if aggr_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            aggr_function__ = Some(map_.next_value::<AggregateFunction>()? as i32);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Distinct => {
                            if distinct__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                        GeneratedField::OrderBy => {
                            if order_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            order_by__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AggregateExprNode {
                    aggr_function: aggr_function__.unwrap_or_default(),
                    expr: expr__.unwrap_or_default(),
                    distinct: distinct__.unwrap_or_default(),
                    filter: filter__,
                    order_by: order_by__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
            Self::ArrayAgg => "ARRAY_AGG",
            Self::Correlation => "CORRELATION",
            Self::Grouping => "GROUPING",
            Self::NthValueAgg => "NTH_VALUE_AGG",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AggregateFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "MIN",
            "MAX",
            "AVG",
            "ARRAY_AGG",
            "CORRELATION",
            "GROUPING",
            "NTH_VALUE_AGG",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "MIN" => Ok(AggregateFunction::Min),
                    "MAX" => Ok(AggregateFunction::Max),
                    "AVG" => Ok(AggregateFunction::Avg),
                    "ARRAY_AGG" => Ok(AggregateFunction::ArrayAgg),
                    "CORRELATION" => Ok(AggregateFunction::Correlation),
                    "GROUPING" => Ok(AggregateFunction::Grouping),
                    "NTH_VALUE_AGG" => Ok(AggregateFunction::NthValueAgg),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Partial => "PARTIAL",
            Self::Final => "FINAL",
            Self::FinalPartitioned => "FINAL_PARTITIONED",
            Self::Single => "SINGLE",
            Self::SinglePartitioned => "SINGLE_PARTITIONED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for AggregateMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "PARTIAL",
            "FINAL",
            "FINAL_PARTITIONED",
            "SINGLE",
            "SINGLE_PARTITIONED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "PARTIAL" => Ok(AggregateMode::Partial),
                    "FINAL" => Ok(AggregateMode::Final),
                    "FINAL_PARTITIONED" => Ok(AggregateMode::FinalPartitioned),
                    "SINGLE" => Ok(AggregateMode::Single),
                    "SINGLE_PARTITIONED" => Ok(AggregateMode::SinglePartitioned),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.group_expr.is_empty() {
            len += 1;
        }
        if !self.aggr_expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.group_expr.is_empty() {
            struct_ser.serialize_field("groupExpr", &self.group_expr)?;
        }
        if !self.aggr_expr.is_empty() {
            struct_ser.serialize_field("aggrExpr", &self.aggr_expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "group_expr",
            "groupExpr",
            "aggr_expr",
            "aggrExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            GroupExpr,
            AggrExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "groupExpr" | "group_expr" => Ok(GeneratedField::GroupExpr),
                            "aggrExpr" | "aggr_expr" => Ok(GeneratedField::AggrExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut group_expr__ = None;
                let mut aggr_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::GroupExpr => {
                            if group_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupExpr"));
                            }
                            group_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AggrExpr => {
                            if aggr_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrExpr"));
                            }
                            aggr_expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AggregateNode {
                    input: input__,
                    group_expr: group_expr__.unwrap_or_default(),
                    aggr_expr: aggr_expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AggregateUdfExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fun_name.is_empty() {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.distinct {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        if !self.order_by.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AggregateUDFExprNode", len)?;
        if !self.fun_name.is_empty() {
            struct_ser.serialize_field("funName", &self.fun_name)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if self.distinct {
            struct_ser.serialize_field("distinct", &self.distinct)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        if !self.order_by.is_empty() {
            struct_ser.serialize_field("orderBy", &self.order_by)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AggregateUdfExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fun_name",
            "funName",
            "args",
            "distinct",
            "filter",
            "order_by",
            "orderBy",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FunName,
            Args,
            Distinct,
            Filter,
            OrderBy,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "funName" | "fun_name" => Ok(GeneratedField::FunName),
                            "args" => Ok(GeneratedField::Args),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "filter" => Ok(GeneratedField::Filter),
                            "orderBy" | "order_by" => Ok(GeneratedField::OrderBy),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AggregateUdfExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AggregateUDFExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AggregateUdfExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fun_name__ = None;
                let mut args__ = None;
                let mut distinct__ = None;
                let mut filter__ = None;
                let mut order_by__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FunName => {
                            if fun_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funName"));
                            }
                            fun_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Distinct => {
                            if distinct__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                        GeneratedField::OrderBy => {
                            if order_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            order_by__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AggregateUdfExprNode {
                    fun_name: fun_name__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    distinct: distinct__.unwrap_or_default(),
                    filter: filter__,
                    order_by: order_by__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AggregateUDFExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AliasNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.alias.is_empty() {
            len += 1;
        }
        if !self.relation.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AliasNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.alias.is_empty() {
            struct_ser.serialize_field("alias", &self.alias)?;
        }
        if !self.relation.is_empty() {
            struct_ser.serialize_field("relation", &self.relation)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AliasNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "alias",
            "relation",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Alias,
            Relation,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "alias" => Ok(GeneratedField::Alias),
                            "relation" => Ok(GeneratedField::Relation),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AliasNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AliasNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AliasNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut alias__ = None;
                let mut relation__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Relation => {
                            if relation__.is_some() {
                                return Err(serde::de::Error::duplicate_field("relation"));
                            }
                            relation__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AliasNode {
                    expr: expr__,
                    alias: alias__.unwrap_or_default(),
                    relation: relation__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AliasNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AnalyzeExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.verbose {
            len += 1;
        }
        if self.show_statistics {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AnalyzeExecNode", len)?;
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        if self.show_statistics {
            struct_ser.serialize_field("showStatistics", &self.show_statistics)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AnalyzeExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "verbose",
            "show_statistics",
            "showStatistics",
            "input",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Verbose,
            ShowStatistics,
            Input,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "verbose" => Ok(GeneratedField::Verbose),
                            "showStatistics" | "show_statistics" => Ok(GeneratedField::ShowStatistics),
                            "input" => Ok(GeneratedField::Input),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AnalyzeExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AnalyzeExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AnalyzeExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut verbose__ = None;
                let mut show_statistics__ = None;
                let mut input__ = None;
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ShowStatistics => {
                            if show_statistics__.is_some() {
                                return Err(serde::de::Error::duplicate_field("showStatistics"));
                            }
                            show_statistics__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AnalyzeExecNode {
                    verbose: verbose__.unwrap_or_default(),
                    show_statistics: show_statistics__.unwrap_or_default(),
                    input: input__,
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AnalyzeExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AnalyzeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.verbose {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AnalyzeNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AnalyzeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "verbose",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Verbose,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "verbose" => Ok(GeneratedField::Verbose),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AnalyzeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AnalyzeNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AnalyzeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut verbose__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AnalyzeNode {
                    input: input__,
                    verbose: verbose__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AnalyzeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AnalyzedLogicalPlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.analyzer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AnalyzedLogicalPlanType", len)?;
        if !self.analyzer_name.is_empty() {
            struct_ser.serialize_field("analyzerName", &self.analyzer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AnalyzedLogicalPlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "analyzer_name",
            "analyzerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            AnalyzerName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "analyzerName" | "analyzer_name" => Ok(GeneratedField::AnalyzerName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AnalyzedLogicalPlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AnalyzedLogicalPlanType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AnalyzedLogicalPlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut analyzer_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::AnalyzerName => {
                            if analyzer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("analyzerName"));
                            }
                            analyzer_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(AnalyzedLogicalPlanType {
                    analyzer_name: analyzer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AnalyzedLogicalPlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for AvroScanExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.base_conf.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.AvroScanExecNode", len)?;
        if let Some(v) = self.base_conf.as_ref() {
            struct_ser.serialize_field("baseConf", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for AvroScanExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "base_conf",
            "baseConf",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BaseConf,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "baseConf" | "base_conf" => Ok(GeneratedField::BaseConf),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = AvroScanExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.AvroScanExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<AvroScanExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut base_conf__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::BaseConf => {
                            if base_conf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseConf"));
                            }
                            base_conf__ = map_.next_value()?;
                        }
                    }
                }
                Ok(AvroScanExecNode {
                    base_conf: base_conf__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.AvroScanExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BareTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.table.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.BareTableReference", len)?;
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BareTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Table,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BareTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.BareTableReference")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BareTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(BareTableReference {
                    table: table__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.BareTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BetweenNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.negated {
            len += 1;
        }
        if self.low.is_some() {
            len += 1;
        }
        if self.high.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.BetweenNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.low.as_ref() {
            struct_ser.serialize_field("low", v)?;
        }
        if let Some(v) = self.high.as_ref() {
            struct_ser.serialize_field("high", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BetweenNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "negated",
            "low",
            "high",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Negated,
            Low,
            High,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "negated" => Ok(GeneratedField::Negated),
                            "low" => Ok(GeneratedField::Low),
                            "high" => Ok(GeneratedField::High),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BetweenNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.BetweenNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BetweenNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut negated__ = None;
                let mut low__ = None;
                let mut high__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Low => {
                            if low__.is_some() {
                                return Err(serde::de::Error::duplicate_field("low"));
                            }
                            low__ = map_.next_value()?;
                        }
                        GeneratedField::High => {
                            if high__.is_some() {
                                return Err(serde::de::Error::duplicate_field("high"));
                            }
                            high__ = map_.next_value()?;
                        }
                    }
                }
                Ok(BetweenNode {
                    expr: expr__,
                    negated: negated__.unwrap_or_default(),
                    low: low__,
                    high: high__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.BetweenNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BinaryExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.operands.is_empty() {
            len += 1;
        }
        if !self.op.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.BinaryExprNode", len)?;
        if !self.operands.is_empty() {
            struct_ser.serialize_field("operands", &self.operands)?;
        }
        if !self.op.is_empty() {
            struct_ser.serialize_field("op", &self.op)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for BinaryExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "operands",
            "op",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Operands,
            Op,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "operands" => Ok(GeneratedField::Operands),
                            "op" => Ok(GeneratedField::Op),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BinaryExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.BinaryExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<BinaryExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut operands__ = None;
                let mut op__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Operands => {
                            if operands__.is_some() {
                                return Err(serde::de::Error::duplicate_field("operands"));
                            }
                            operands__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Op => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("op"));
                            }
                            op__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(BinaryExprNode {
                    operands: operands__.unwrap_or_default(),
                    op: op__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.BinaryExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for BuiltInWindowFunction {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::RowNumber => "ROW_NUMBER",
            Self::Rank => "RANK",
            Self::DenseRank => "DENSE_RANK",
            Self::PercentRank => "PERCENT_RANK",
            Self::CumeDist => "CUME_DIST",
            Self::Ntile => "NTILE",
            Self::Lag => "LAG",
            Self::Lead => "LEAD",
            Self::FirstValue => "FIRST_VALUE",
            Self::LastValue => "LAST_VALUE",
            Self::NthValue => "NTH_VALUE",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for BuiltInWindowFunction {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "PERCENT_RANK",
            "CUME_DIST",
            "NTILE",
            "LAG",
            "LEAD",
            "FIRST_VALUE",
            "LAST_VALUE",
            "NTH_VALUE",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = BuiltInWindowFunction;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ROW_NUMBER" => Ok(BuiltInWindowFunction::RowNumber),
                    "RANK" => Ok(BuiltInWindowFunction::Rank),
                    "DENSE_RANK" => Ok(BuiltInWindowFunction::DenseRank),
                    "PERCENT_RANK" => Ok(BuiltInWindowFunction::PercentRank),
                    "CUME_DIST" => Ok(BuiltInWindowFunction::CumeDist),
                    "NTILE" => Ok(BuiltInWindowFunction::Ntile),
                    "LAG" => Ok(BuiltInWindowFunction::Lag),
                    "LEAD" => Ok(BuiltInWindowFunction::Lead),
                    "FIRST_VALUE" => Ok(BuiltInWindowFunction::FirstValue),
                    "LAST_VALUE" => Ok(BuiltInWindowFunction::LastValue),
                    "NTH_VALUE" => Ok(BuiltInWindowFunction::NthValue),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for CaseNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.when_then_expr.is_empty() {
            len += 1;
        }
        if self.else_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CaseNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.when_then_expr.is_empty() {
            struct_ser.serialize_field("whenThenExpr", &self.when_then_expr)?;
        }
        if let Some(v) = self.else_expr.as_ref() {
            struct_ser.serialize_field("elseExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CaseNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "when_then_expr",
            "whenThenExpr",
            "else_expr",
            "elseExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            WhenThenExpr,
            ElseExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "whenThenExpr" | "when_then_expr" => Ok(GeneratedField::WhenThenExpr),
                            "elseExpr" | "else_expr" => Ok(GeneratedField::ElseExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CaseNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CaseNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CaseNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut when_then_expr__ = None;
                let mut else_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::WhenThenExpr => {
                            if when_then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenThenExpr"));
                            }
                            when_then_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ElseExpr => {
                            if else_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("elseExpr"));
                            }
                            else_expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CaseNode {
                    expr: expr__,
                    when_then_expr: when_then_expr__.unwrap_or_default(),
                    else_expr: else_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CaseNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CastNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CoalesceBatchesExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.target_batch_size != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CoalesceBatchesExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.target_batch_size != 0 {
            struct_ser.serialize_field("targetBatchSize", &self.target_batch_size)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CoalesceBatchesExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "target_batch_size",
            "targetBatchSize",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            TargetBatchSize,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "targetBatchSize" | "target_batch_size" => Ok(GeneratedField::TargetBatchSize),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CoalesceBatchesExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CoalesceBatchesExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CoalesceBatchesExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut target_batch_size__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::TargetBatchSize => {
                            if target_batch_size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetBatchSize"));
                            }
                            target_batch_size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CoalesceBatchesExecNode {
                    input: input__,
                    target_batch_size: target_batch_size__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CoalesceBatchesExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CoalescePartitionsExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CoalescePartitionsExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CoalescePartitionsExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CoalescePartitionsExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CoalescePartitionsExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CoalescePartitionsExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CoalescePartitionsExecNode {
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CoalescePartitionsExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ColumnIndex {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.index != 0 {
            len += 1;
        }
        if self.side != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ColumnIndex", len)?;
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        if self.side != 0 {
            let v = super::datafusion_common::JoinSide::try_from(self.side)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.side)))?;
            struct_ser.serialize_field("side", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ColumnIndex {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "index",
            "side",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Index,
            Side,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "index" => Ok(GeneratedField::Index),
                            "side" => Ok(GeneratedField::Side),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ColumnIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ColumnIndex")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ColumnIndex, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut index__ = None;
                let mut side__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Side => {
                            if side__.is_some() {
                                return Err(serde::de::Error::duplicate_field("side"));
                            }
                            side__ = Some(map_.next_value::<super::datafusion_common::JoinSide>()? as i32);
                        }
                    }
                }
                Ok(ColumnIndex {
                    index: index__.unwrap_or_default(),
                    side: side__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ColumnIndex", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CopyToNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.output_url.is_empty() {
            len += 1;
        }
        if !self.partition_by.is_empty() {
            len += 1;
        }
        if self.format_options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CopyToNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.output_url.is_empty() {
            struct_ser.serialize_field("outputUrl", &self.output_url)?;
        }
        if !self.partition_by.is_empty() {
            struct_ser.serialize_field("partitionBy", &self.partition_by)?;
        }
        if let Some(v) = self.format_options.as_ref() {
            match v {
                copy_to_node::FormatOptions::Csv(v) => {
                    struct_ser.serialize_field("csv", v)?;
                }
                copy_to_node::FormatOptions::Json(v) => {
                    struct_ser.serialize_field("json", v)?;
                }
                copy_to_node::FormatOptions::Parquet(v) => {
                    struct_ser.serialize_field("parquet", v)?;
                }
                copy_to_node::FormatOptions::Avro(v) => {
                    struct_ser.serialize_field("avro", v)?;
                }
                copy_to_node::FormatOptions::Arrow(v) => {
                    struct_ser.serialize_field("arrow", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CopyToNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "output_url",
            "outputUrl",
            "partition_by",
            "partitionBy",
            "csv",
            "json",
            "parquet",
            "avro",
            "arrow",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            OutputUrl,
            PartitionBy,
            Csv,
            Json,
            Parquet,
            Avro,
            Arrow,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "outputUrl" | "output_url" => Ok(GeneratedField::OutputUrl),
                            "partitionBy" | "partition_by" => Ok(GeneratedField::PartitionBy),
                            "csv" => Ok(GeneratedField::Csv),
                            "json" => Ok(GeneratedField::Json),
                            "parquet" => Ok(GeneratedField::Parquet),
                            "avro" => Ok(GeneratedField::Avro),
                            "arrow" => Ok(GeneratedField::Arrow),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CopyToNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CopyToNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CopyToNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut output_url__ = None;
                let mut partition_by__ = None;
                let mut format_options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::OutputUrl => {
                            if output_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputUrl"));
                            }
                            output_url__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PartitionBy => {
                            if partition_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionBy"));
                            }
                            partition_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Csv => {
                            if format_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csv"));
                            }
                            format_options__ = map_.next_value::<::std::option::Option<_>>()?.map(copy_to_node::FormatOptions::Csv)
;
                        }
                        GeneratedField::Json => {
                            if format_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("json"));
                            }
                            format_options__ = map_.next_value::<::std::option::Option<_>>()?.map(copy_to_node::FormatOptions::Json)
;
                        }
                        GeneratedField::Parquet => {
                            if format_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquet"));
                            }
                            format_options__ = map_.next_value::<::std::option::Option<_>>()?.map(copy_to_node::FormatOptions::Parquet)
;
                        }
                        GeneratedField::Avro => {
                            if format_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("avro"));
                            }
                            format_options__ = map_.next_value::<::std::option::Option<_>>()?.map(copy_to_node::FormatOptions::Avro)
;
                        }
                        GeneratedField::Arrow => {
                            if format_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrow"));
                            }
                            format_options__ = map_.next_value::<::std::option::Option<_>>()?.map(copy_to_node::FormatOptions::Arrow)
;
                        }
                    }
                }
                Ok(CopyToNode {
                    input: input__,
                    output_url: output_url__.unwrap_or_default(),
                    partition_by: partition_by__.unwrap_or_default(),
                    format_options: format_options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CopyToNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateCatalogNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.catalog_name.is_empty() {
            len += 1;
        }
        if self.if_not_exists {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateCatalogNode", len)?;
        if !self.catalog_name.is_empty() {
            struct_ser.serialize_field("catalogName", &self.catalog_name)?;
        }
        if self.if_not_exists {
            struct_ser.serialize_field("ifNotExists", &self.if_not_exists)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateCatalogNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "catalog_name",
            "catalogName",
            "if_not_exists",
            "ifNotExists",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CatalogName,
            IfNotExists,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "catalogName" | "catalog_name" => Ok(GeneratedField::CatalogName),
                            "ifNotExists" | "if_not_exists" => Ok(GeneratedField::IfNotExists),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateCatalogNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateCatalogNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateCatalogNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut catalog_name__ = None;
                let mut if_not_exists__ = None;
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::CatalogName => {
                            if catalog_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("catalogName"));
                            }
                            catalog_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IfNotExists => {
                            if if_not_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifNotExists"));
                            }
                            if_not_exists__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CreateCatalogNode {
                    catalog_name: catalog_name__.unwrap_or_default(),
                    if_not_exists: if_not_exists__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateCatalogNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateCatalogSchemaNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.schema_name.is_empty() {
            len += 1;
        }
        if self.if_not_exists {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateCatalogSchemaNode", len)?;
        if !self.schema_name.is_empty() {
            struct_ser.serialize_field("schemaName", &self.schema_name)?;
        }
        if self.if_not_exists {
            struct_ser.serialize_field("ifNotExists", &self.if_not_exists)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateCatalogSchemaNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema_name",
            "schemaName",
            "if_not_exists",
            "ifNotExists",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SchemaName,
            IfNotExists,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schemaName" | "schema_name" => Ok(GeneratedField::SchemaName),
                            "ifNotExists" | "if_not_exists" => Ok(GeneratedField::IfNotExists),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateCatalogSchemaNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateCatalogSchemaNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateCatalogSchemaNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema_name__ = None;
                let mut if_not_exists__ = None;
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SchemaName => {
                            if schema_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schemaName"));
                            }
                            schema_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IfNotExists => {
                            if if_not_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifNotExists"));
                            }
                            if_not_exists__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CreateCatalogSchemaNode {
                    schema_name: schema_name__.unwrap_or_default(),
                    if_not_exists: if_not_exists__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateCatalogSchemaNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateExternalTableNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if !self.location.is_empty() {
            len += 1;
        }
        if !self.file_type.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if self.if_not_exists {
            len += 1;
        }
        if !self.definition.is_empty() {
            len += 1;
        }
        if !self.order_exprs.is_empty() {
            len += 1;
        }
        if self.unbounded {
            len += 1;
        }
        if !self.options.is_empty() {
            len += 1;
        }
        if self.constraints.is_some() {
            len += 1;
        }
        if !self.column_defaults.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateExternalTableNode", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if !self.location.is_empty() {
            struct_ser.serialize_field("location", &self.location)?;
        }
        if !self.file_type.is_empty() {
            struct_ser.serialize_field("fileType", &self.file_type)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if self.if_not_exists {
            struct_ser.serialize_field("ifNotExists", &self.if_not_exists)?;
        }
        if !self.definition.is_empty() {
            struct_ser.serialize_field("definition", &self.definition)?;
        }
        if !self.order_exprs.is_empty() {
            struct_ser.serialize_field("orderExprs", &self.order_exprs)?;
        }
        if self.unbounded {
            struct_ser.serialize_field("unbounded", &self.unbounded)?;
        }
        if !self.options.is_empty() {
            struct_ser.serialize_field("options", &self.options)?;
        }
        if let Some(v) = self.constraints.as_ref() {
            struct_ser.serialize_field("constraints", v)?;
        }
        if !self.column_defaults.is_empty() {
            struct_ser.serialize_field("columnDefaults", &self.column_defaults)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateExternalTableNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "location",
            "file_type",
            "fileType",
            "schema",
            "table_partition_cols",
            "tablePartitionCols",
            "if_not_exists",
            "ifNotExists",
            "definition",
            "order_exprs",
            "orderExprs",
            "unbounded",
            "options",
            "constraints",
            "column_defaults",
            "columnDefaults",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Location,
            FileType,
            Schema,
            TablePartitionCols,
            IfNotExists,
            Definition,
            OrderExprs,
            Unbounded,
            Options,
            Constraints,
            ColumnDefaults,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "location" => Ok(GeneratedField::Location),
                            "fileType" | "file_type" => Ok(GeneratedField::FileType),
                            "schema" => Ok(GeneratedField::Schema),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "ifNotExists" | "if_not_exists" => Ok(GeneratedField::IfNotExists),
                            "definition" => Ok(GeneratedField::Definition),
                            "orderExprs" | "order_exprs" => Ok(GeneratedField::OrderExprs),
                            "unbounded" => Ok(GeneratedField::Unbounded),
                            "options" => Ok(GeneratedField::Options),
                            "constraints" => Ok(GeneratedField::Constraints),
                            "columnDefaults" | "column_defaults" => Ok(GeneratedField::ColumnDefaults),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateExternalTableNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateExternalTableNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateExternalTableNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut location__ = None;
                let mut file_type__ = None;
                let mut schema__ = None;
                let mut table_partition_cols__ = None;
                let mut if_not_exists__ = None;
                let mut definition__ = None;
                let mut order_exprs__ = None;
                let mut unbounded__ = None;
                let mut options__ = None;
                let mut constraints__ = None;
                let mut column_defaults__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = map_.next_value()?;
                        }
                        GeneratedField::Location => {
                            if location__.is_some() {
                                return Err(serde::de::Error::duplicate_field("location"));
                            }
                            location__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FileType => {
                            if file_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileType"));
                            }
                            file_type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map_.next_value()?);
                        }
                        GeneratedField::IfNotExists => {
                            if if_not_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifNotExists"));
                            }
                            if_not_exists__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Definition => {
                            if definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("definition"));
                            }
                            definition__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OrderExprs => {
                            if order_exprs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderExprs"));
                            }
                            order_exprs__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Unbounded => {
                            if unbounded__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unbounded"));
                            }
                            unbounded__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::Constraints => {
                            if constraints__.is_some() {
                                return Err(serde::de::Error::duplicate_field("constraints"));
                            }
                            constraints__ = map_.next_value()?;
                        }
                        GeneratedField::ColumnDefaults => {
                            if column_defaults__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnDefaults"));
                            }
                            column_defaults__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                    }
                }
                Ok(CreateExternalTableNode {
                    name: name__,
                    location: location__.unwrap_or_default(),
                    file_type: file_type__.unwrap_or_default(),
                    schema: schema__,
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    if_not_exists: if_not_exists__.unwrap_or_default(),
                    definition: definition__.unwrap_or_default(),
                    order_exprs: order_exprs__.unwrap_or_default(),
                    unbounded: unbounded__.unwrap_or_default(),
                    options: options__.unwrap_or_default(),
                    constraints: constraints__,
                    column_defaults: column_defaults__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateExternalTableNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CreateViewNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if self.or_replace {
            len += 1;
        }
        if !self.definition.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CreateViewNode", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.or_replace {
            struct_ser.serialize_field("orReplace", &self.or_replace)?;
        }
        if !self.definition.is_empty() {
            struct_ser.serialize_field("definition", &self.definition)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CreateViewNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "input",
            "or_replace",
            "orReplace",
            "definition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Input,
            OrReplace,
            Definition,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "input" => Ok(GeneratedField::Input),
                            "orReplace" | "or_replace" => Ok(GeneratedField::OrReplace),
                            "definition" => Ok(GeneratedField::Definition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CreateViewNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CreateViewNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CreateViewNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut input__ = None;
                let mut or_replace__ = None;
                let mut definition__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = map_.next_value()?;
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::OrReplace => {
                            if or_replace__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orReplace"));
                            }
                            or_replace__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Definition => {
                            if definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("definition"));
                            }
                            definition__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CreateViewNode {
                    name: name__,
                    input: input__,
                    or_replace: or_replace__.unwrap_or_default(),
                    definition: definition__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CreateViewNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CrossJoinExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CrossJoinExecNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CrossJoinExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CrossJoinExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CrossJoinExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CrossJoinExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CrossJoinExecNode {
                    left: left__,
                    right: right__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CrossJoinExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CrossJoinNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CrossJoinNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CrossJoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CrossJoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CrossJoinNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CrossJoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CrossJoinNode {
                    left: left__,
                    right: right__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CrossJoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvScanExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.base_conf.is_some() {
            len += 1;
        }
        if self.has_header {
            len += 1;
        }
        if !self.delimiter.is_empty() {
            len += 1;
        }
        if !self.quote.is_empty() {
            len += 1;
        }
        if self.optional_escape.is_some() {
            len += 1;
        }
        if self.optional_comment.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CsvScanExecNode", len)?;
        if let Some(v) = self.base_conf.as_ref() {
            struct_ser.serialize_field("baseConf", v)?;
        }
        if self.has_header {
            struct_ser.serialize_field("hasHeader", &self.has_header)?;
        }
        if !self.delimiter.is_empty() {
            struct_ser.serialize_field("delimiter", &self.delimiter)?;
        }
        if !self.quote.is_empty() {
            struct_ser.serialize_field("quote", &self.quote)?;
        }
        if let Some(v) = self.optional_escape.as_ref() {
            match v {
                csv_scan_exec_node::OptionalEscape::Escape(v) => {
                    struct_ser.serialize_field("escape", v)?;
                }
            }
        }
        if let Some(v) = self.optional_comment.as_ref() {
            match v {
                csv_scan_exec_node::OptionalComment::Comment(v) => {
                    struct_ser.serialize_field("comment", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvScanExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "base_conf",
            "baseConf",
            "has_header",
            "hasHeader",
            "delimiter",
            "quote",
            "escape",
            "comment",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BaseConf,
            HasHeader,
            Delimiter,
            Quote,
            Escape,
            Comment,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "baseConf" | "base_conf" => Ok(GeneratedField::BaseConf),
                            "hasHeader" | "has_header" => Ok(GeneratedField::HasHeader),
                            "delimiter" => Ok(GeneratedField::Delimiter),
                            "quote" => Ok(GeneratedField::Quote),
                            "escape" => Ok(GeneratedField::Escape),
                            "comment" => Ok(GeneratedField::Comment),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvScanExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CsvScanExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CsvScanExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut base_conf__ = None;
                let mut has_header__ = None;
                let mut delimiter__ = None;
                let mut quote__ = None;
                let mut optional_escape__ = None;
                let mut optional_comment__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::BaseConf => {
                            if base_conf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseConf"));
                            }
                            base_conf__ = map_.next_value()?;
                        }
                        GeneratedField::HasHeader => {
                            if has_header__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hasHeader"));
                            }
                            has_header__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Delimiter => {
                            if delimiter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("delimiter"));
                            }
                            delimiter__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Quote => {
                            if quote__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quote"));
                            }
                            quote__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Escape => {
                            if optional_escape__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escape"));
                            }
                            optional_escape__ = map_.next_value::<::std::option::Option<_>>()?.map(csv_scan_exec_node::OptionalEscape::Escape);
                        }
                        GeneratedField::Comment => {
                            if optional_comment__.is_some() {
                                return Err(serde::de::Error::duplicate_field("comment"));
                            }
                            optional_comment__ = map_.next_value::<::std::option::Option<_>>()?.map(csv_scan_exec_node::OptionalComment::Comment);
                        }
                    }
                }
                Ok(CsvScanExecNode {
                    base_conf: base_conf__,
                    has_header: has_header__.unwrap_or_default(),
                    delimiter: delimiter__.unwrap_or_default(),
                    quote: quote__.unwrap_or_default(),
                    optional_escape: optional_escape__,
                    optional_comment: optional_comment__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CsvScanExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvSink {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.config.is_some() {
            len += 1;
        }
        if self.writer_options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CsvSink", len)?;
        if let Some(v) = self.config.as_ref() {
            struct_ser.serialize_field("config", v)?;
        }
        if let Some(v) = self.writer_options.as_ref() {
            struct_ser.serialize_field("writerOptions", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvSink {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "config",
            "writer_options",
            "writerOptions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Config,
            WriterOptions,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "config" => Ok(GeneratedField::Config),
                            "writerOptions" | "writer_options" => Ok(GeneratedField::WriterOptions),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvSink;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CsvSink")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CsvSink, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut config__ = None;
                let mut writer_options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Config => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config__ = map_.next_value()?;
                        }
                        GeneratedField::WriterOptions => {
                            if writer_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writerOptions"));
                            }
                            writer_options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CsvSink {
                    config: config__,
                    writer_options: writer_options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CsvSink", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CsvSinkExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.sink.is_some() {
            len += 1;
        }
        if self.sink_schema.is_some() {
            len += 1;
        }
        if self.sort_order.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CsvSinkExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.sink.as_ref() {
            struct_ser.serialize_field("sink", v)?;
        }
        if let Some(v) = self.sink_schema.as_ref() {
            struct_ser.serialize_field("sinkSchema", v)?;
        }
        if let Some(v) = self.sort_order.as_ref() {
            struct_ser.serialize_field("sortOrder", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CsvSinkExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "sink",
            "sink_schema",
            "sinkSchema",
            "sort_order",
            "sortOrder",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Sink,
            SinkSchema,
            SortOrder,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "sink" => Ok(GeneratedField::Sink),
                            "sinkSchema" | "sink_schema" => Ok(GeneratedField::SinkSchema),
                            "sortOrder" | "sort_order" => Ok(GeneratedField::SortOrder),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CsvSinkExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CsvSinkExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CsvSinkExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut sink__ = None;
                let mut sink_schema__ = None;
                let mut sort_order__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Sink => {
                            if sink__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sink"));
                            }
                            sink__ = map_.next_value()?;
                        }
                        GeneratedField::SinkSchema => {
                            if sink_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sinkSchema"));
                            }
                            sink_schema__ = map_.next_value()?;
                        }
                        GeneratedField::SortOrder => {
                            if sort_order__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortOrder"));
                            }
                            sort_order__ = map_.next_value()?;
                        }
                    }
                }
                Ok(CsvSinkExecNode {
                    input: input__,
                    sink: sink__,
                    sink_schema: sink_schema__,
                    sort_order: sort_order__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CsvSinkExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CubeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CubeNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CubeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CubeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CubeNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CubeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(CubeNode {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CubeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CustomTableScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_name.is_some() {
            len += 1;
        }
        if self.projection.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.filters.is_empty() {
            len += 1;
        }
        if !self.custom_table_data.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.CustomTableScanNode", len)?;
        if let Some(v) = self.table_name.as_ref() {
            struct_ser.serialize_field("tableName", v)?;
        }
        if let Some(v) = self.projection.as_ref() {
            struct_ser.serialize_field("projection", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.filters.is_empty() {
            struct_ser.serialize_field("filters", &self.filters)?;
        }
        if !self.custom_table_data.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("customTableData", pbjson::private::base64::encode(&self.custom_table_data).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CustomTableScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name",
            "tableName",
            "projection",
            "schema",
            "filters",
            "custom_table_data",
            "customTableData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableName,
            Projection,
            Schema,
            Filters,
            CustomTableData,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "projection" => Ok(GeneratedField::Projection),
                            "schema" => Ok(GeneratedField::Schema),
                            "filters" => Ok(GeneratedField::Filters),
                            "customTableData" | "custom_table_data" => Ok(GeneratedField::CustomTableData),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CustomTableScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.CustomTableScanNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CustomTableScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name__ = None;
                let mut projection__ = None;
                let mut schema__ = None;
                let mut filters__ = None;
                let mut custom_table_data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = map_.next_value()?;
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = map_.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Filters => {
                            if filters__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filters"));
                            }
                            filters__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CustomTableData => {
                            if custom_table_data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("customTableData"));
                            }
                            custom_table_data__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(CustomTableScanNode {
                    table_name: table_name__,
                    projection: projection__,
                    schema: schema__,
                    filters: filters__.unwrap_or_default(),
                    custom_table_data: custom_table_data__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.CustomTableScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DateUnit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Day => "Day",
            Self::DateMillisecond => "DateMillisecond",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for DateUnit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "Day",
            "DateMillisecond",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DateUnit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "Day" => Ok(DateUnit::Day),
                    "DateMillisecond" => Ok(DateUnit::DateMillisecond),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for DistinctNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.DistinctNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DistinctNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DistinctNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.DistinctNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DistinctNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                    }
                }
                Ok(DistinctNode {
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.DistinctNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DistinctOnNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.on_expr.is_empty() {
            len += 1;
        }
        if !self.select_expr.is_empty() {
            len += 1;
        }
        if !self.sort_expr.is_empty() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.DistinctOnNode", len)?;
        if !self.on_expr.is_empty() {
            struct_ser.serialize_field("onExpr", &self.on_expr)?;
        }
        if !self.select_expr.is_empty() {
            struct_ser.serialize_field("selectExpr", &self.select_expr)?;
        }
        if !self.sort_expr.is_empty() {
            struct_ser.serialize_field("sortExpr", &self.sort_expr)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DistinctOnNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "on_expr",
            "onExpr",
            "select_expr",
            "selectExpr",
            "sort_expr",
            "sortExpr",
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OnExpr,
            SelectExpr,
            SortExpr,
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "onExpr" | "on_expr" => Ok(GeneratedField::OnExpr),
                            "selectExpr" | "select_expr" => Ok(GeneratedField::SelectExpr),
                            "sortExpr" | "sort_expr" => Ok(GeneratedField::SortExpr),
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DistinctOnNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.DistinctOnNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DistinctOnNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut on_expr__ = None;
                let mut select_expr__ = None;
                let mut sort_expr__ = None;
                let mut input__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OnExpr => {
                            if on_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("onExpr"));
                            }
                            on_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SelectExpr => {
                            if select_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selectExpr"));
                            }
                            select_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SortExpr => {
                            if sort_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortExpr"));
                            }
                            sort_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                    }
                }
                Ok(DistinctOnNode {
                    on_expr: on_expr__.unwrap_or_default(),
                    select_expr: select_expr__.unwrap_or_default(),
                    sort_expr: sort_expr__.unwrap_or_default(),
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.DistinctOnNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for DropViewNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        if self.if_exists {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.DropViewNode", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        if self.if_exists {
            struct_ser.serialize_field("ifExists", &self.if_exists)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for DropViewNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "if_exists",
            "ifExists",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            IfExists,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "ifExists" | "if_exists" => Ok(GeneratedField::IfExists),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = DropViewNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.DropViewNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<DropViewNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut if_exists__ = None;
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = map_.next_value()?;
                        }
                        GeneratedField::IfExists => {
                            if if_exists__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ifExists"));
                            }
                            if_exists__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(DropViewNode {
                    name: name__,
                    if_exists: if_exists__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.DropViewNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EmptyExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.EmptyExecNode", len)?;
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EmptyExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EmptyExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.EmptyExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EmptyExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(EmptyExecNode {
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.EmptyExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for EmptyRelationNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.produce_one_row {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.EmptyRelationNode", len)?;
        if self.produce_one_row {
            struct_ser.serialize_field("produceOneRow", &self.produce_one_row)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for EmptyRelationNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "produce_one_row",
            "produceOneRow",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ProduceOneRow,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "produceOneRow" | "produce_one_row" => Ok(GeneratedField::ProduceOneRow),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = EmptyRelationNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.EmptyRelationNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<EmptyRelationNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut produce_one_row__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ProduceOneRow => {
                            if produce_one_row__.is_some() {
                                return Err(serde::de::Error::duplicate_field("produceOneRow"));
                            }
                            produce_one_row__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(EmptyRelationNode {
                    produce_one_row: produce_one_row__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.EmptyRelationNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExplainExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema.is_some() {
            len += 1;
        }
        if !self.stringified_plans.is_empty() {
            len += 1;
        }
        if self.verbose {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExplainExecNode", len)?;
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.stringified_plans.is_empty() {
            struct_ser.serialize_field("stringifiedPlans", &self.stringified_plans)?;
        }
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExplainExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
            "stringified_plans",
            "stringifiedPlans",
            "verbose",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
            StringifiedPlans,
            Verbose,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schema" => Ok(GeneratedField::Schema),
                            "stringifiedPlans" | "stringified_plans" => Ok(GeneratedField::StringifiedPlans),
                            "verbose" => Ok(GeneratedField::Verbose),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExplainExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExplainExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ExplainExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema__ = None;
                let mut stringified_plans__ = None;
                let mut verbose__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::StringifiedPlans => {
                            if stringified_plans__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stringifiedPlans"));
                            }
                            stringified_plans__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ExplainExecNode {
                    schema: schema__,
                    stringified_plans: stringified_plans__.unwrap_or_default(),
                    verbose: verbose__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExplainExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ExplainNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.verbose {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ExplainNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.verbose {
            struct_ser.serialize_field("verbose", &self.verbose)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ExplainNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "verbose",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Verbose,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "verbose" => Ok(GeneratedField::Verbose),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ExplainNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ExplainNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ExplainNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut verbose__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Verbose => {
                            if verbose__.is_some() {
                                return Err(serde::de::Error::duplicate_field("verbose"));
                            }
                            verbose__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ExplainNode {
                    input: input__,
                    verbose: verbose__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ExplainNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileGroup {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.files.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileGroup", len)?;
        if !self.files.is_empty() {
            struct_ser.serialize_field("files", &self.files)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileGroup {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "files",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Files,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "files" => Ok(GeneratedField::Files),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileGroup;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileGroup")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FileGroup, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut files__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Files => {
                            if files__.is_some() {
                                return Err(serde::de::Error::duplicate_field("files"));
                            }
                            files__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FileGroup {
                    files: files__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileGroup", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start != 0 {
            len += 1;
        }
        if self.end != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileRange", len)?;
        if self.start != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("start", ToString::to_string(&self.start).as_str())?;
        }
        if self.end != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("end", ToString::to_string(&self.end).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "end",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            End,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "end" => Ok(GeneratedField::End),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileRange")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FileRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut end__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::End => {
                            if end__.is_some() {
                                return Err(serde::de::Error::duplicate_field("end"));
                            }
                            end__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FileRange {
                    start: start__.unwrap_or_default(),
                    end: end__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileScanExecConf {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.file_groups.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.projection.is_empty() {
            len += 1;
        }
        if self.limit.is_some() {
            len += 1;
        }
        if self.statistics.is_some() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if !self.object_store_url.is_empty() {
            len += 1;
        }
        if !self.output_ordering.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileScanExecConf", len)?;
        if !self.file_groups.is_empty() {
            struct_ser.serialize_field("fileGroups", &self.file_groups)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.projection.is_empty() {
            struct_ser.serialize_field("projection", &self.projection)?;
        }
        if let Some(v) = self.limit.as_ref() {
            struct_ser.serialize_field("limit", v)?;
        }
        if let Some(v) = self.statistics.as_ref() {
            struct_ser.serialize_field("statistics", v)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if !self.object_store_url.is_empty() {
            struct_ser.serialize_field("objectStoreUrl", &self.object_store_url)?;
        }
        if !self.output_ordering.is_empty() {
            struct_ser.serialize_field("outputOrdering", &self.output_ordering)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileScanExecConf {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "file_groups",
            "fileGroups",
            "schema",
            "projection",
            "limit",
            "statistics",
            "table_partition_cols",
            "tablePartitionCols",
            "object_store_url",
            "objectStoreUrl",
            "output_ordering",
            "outputOrdering",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FileGroups,
            Schema,
            Projection,
            Limit,
            Statistics,
            TablePartitionCols,
            ObjectStoreUrl,
            OutputOrdering,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "fileGroups" | "file_groups" => Ok(GeneratedField::FileGroups),
                            "schema" => Ok(GeneratedField::Schema),
                            "projection" => Ok(GeneratedField::Projection),
                            "limit" => Ok(GeneratedField::Limit),
                            "statistics" => Ok(GeneratedField::Statistics),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "objectStoreUrl" | "object_store_url" => Ok(GeneratedField::ObjectStoreUrl),
                            "outputOrdering" | "output_ordering" => Ok(GeneratedField::OutputOrdering),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileScanExecConf;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileScanExecConf")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FileScanExecConf, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut file_groups__ = None;
                let mut schema__ = None;
                let mut projection__ = None;
                let mut limit__ = None;
                let mut statistics__ = None;
                let mut table_partition_cols__ = None;
                let mut object_store_url__ = None;
                let mut output_ordering__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FileGroups => {
                            if file_groups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileGroups"));
                            }
                            file_groups__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = map_.next_value()?;
                        }
                        GeneratedField::Statistics => {
                            if statistics__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statistics"));
                            }
                            statistics__ = map_.next_value()?;
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ObjectStoreUrl => {
                            if object_store_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("objectStoreUrl"));
                            }
                            object_store_url__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OutputOrdering => {
                            if output_ordering__.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputOrdering"));
                            }
                            output_ordering__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FileScanExecConf {
                    file_groups: file_groups__.unwrap_or_default(),
                    schema: schema__,
                    projection: projection__.unwrap_or_default(),
                    limit: limit__,
                    statistics: statistics__,
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    object_store_url: object_store_url__.unwrap_or_default(),
                    output_ordering: output_ordering__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileScanExecConf", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FileSinkConfig {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.object_store_url.is_empty() {
            len += 1;
        }
        if !self.file_groups.is_empty() {
            len += 1;
        }
        if !self.table_paths.is_empty() {
            len += 1;
        }
        if self.output_schema.is_some() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if self.overwrite {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FileSinkConfig", len)?;
        if !self.object_store_url.is_empty() {
            struct_ser.serialize_field("objectStoreUrl", &self.object_store_url)?;
        }
        if !self.file_groups.is_empty() {
            struct_ser.serialize_field("fileGroups", &self.file_groups)?;
        }
        if !self.table_paths.is_empty() {
            struct_ser.serialize_field("tablePaths", &self.table_paths)?;
        }
        if let Some(v) = self.output_schema.as_ref() {
            struct_ser.serialize_field("outputSchema", v)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if self.overwrite {
            struct_ser.serialize_field("overwrite", &self.overwrite)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FileSinkConfig {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "object_store_url",
            "objectStoreUrl",
            "file_groups",
            "fileGroups",
            "table_paths",
            "tablePaths",
            "output_schema",
            "outputSchema",
            "table_partition_cols",
            "tablePartitionCols",
            "overwrite",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ObjectStoreUrl,
            FileGroups,
            TablePaths,
            OutputSchema,
            TablePartitionCols,
            Overwrite,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "objectStoreUrl" | "object_store_url" => Ok(GeneratedField::ObjectStoreUrl),
                            "fileGroups" | "file_groups" => Ok(GeneratedField::FileGroups),
                            "tablePaths" | "table_paths" => Ok(GeneratedField::TablePaths),
                            "outputSchema" | "output_schema" => Ok(GeneratedField::OutputSchema),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "overwrite" => Ok(GeneratedField::Overwrite),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FileSinkConfig;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FileSinkConfig")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FileSinkConfig, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut object_store_url__ = None;
                let mut file_groups__ = None;
                let mut table_paths__ = None;
                let mut output_schema__ = None;
                let mut table_partition_cols__ = None;
                let mut overwrite__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ObjectStoreUrl => {
                            if object_store_url__.is_some() {
                                return Err(serde::de::Error::duplicate_field("objectStoreUrl"));
                            }
                            object_store_url__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FileGroups => {
                            if file_groups__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileGroups"));
                            }
                            file_groups__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TablePaths => {
                            if table_paths__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePaths"));
                            }
                            table_paths__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OutputSchema => {
                            if output_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("outputSchema"));
                            }
                            output_schema__ = map_.next_value()?;
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Overwrite => {
                            if overwrite__.is_some() {
                                return Err(serde::de::Error::duplicate_field("overwrite"));
                            }
                            overwrite__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FileSinkConfig {
                    object_store_url: object_store_url__.unwrap_or_default(),
                    file_groups: file_groups__.unwrap_or_default(),
                    table_paths: table_paths__.unwrap_or_default(),
                    output_schema: output_schema__,
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    overwrite: overwrite__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FileSinkConfig", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FilterExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.default_filter_selectivity != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FilterExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.default_filter_selectivity != 0 {
            struct_ser.serialize_field("defaultFilterSelectivity", &self.default_filter_selectivity)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FilterExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "default_filter_selectivity",
            "defaultFilterSelectivity",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            DefaultFilterSelectivity,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "defaultFilterSelectivity" | "default_filter_selectivity" => Ok(GeneratedField::DefaultFilterSelectivity),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FilterExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FilterExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FilterExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut default_filter_selectivity__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::DefaultFilterSelectivity => {
                            if default_filter_selectivity__.is_some() {
                                return Err(serde::de::Error::duplicate_field("defaultFilterSelectivity"));
                            }
                            default_filter_selectivity__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FilterExecNode {
                    input: input__,
                    expr: expr__,
                    default_filter_selectivity: default_filter_selectivity__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FilterExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FixedSizeBinary {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.length != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FixedSizeBinary", len)?;
        if self.length != 0 {
            struct_ser.serialize_field("length", &self.length)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FixedSizeBinary {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "length",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Length,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "length" => Ok(GeneratedField::Length),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FixedSizeBinary;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FixedSizeBinary")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FixedSizeBinary, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut length__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Length => {
                            if length__.is_some() {
                                return Err(serde::de::Error::duplicate_field("length"));
                            }
                            length__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(FixedSizeBinary {
                    length: length__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FixedSizeBinary", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for FullTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.catalog.is_empty() {
            len += 1;
        }
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.FullTableReference", len)?;
        if !self.catalog.is_empty() {
            struct_ser.serialize_field("catalog", &self.catalog)?;
        }
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for FullTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "catalog",
            "schema",
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Catalog,
            Schema,
            Table,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "catalog" => Ok(GeneratedField::Catalog),
                            "schema" => Ok(GeneratedField::Schema),
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = FullTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.FullTableReference")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<FullTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut catalog__ = None;
                let mut schema__ = None;
                let mut table__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Catalog => {
                            if catalog__.is_some() {
                                return Err(serde::de::Error::duplicate_field("catalog"));
                            }
                            catalog__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(FullTableReference {
                    catalog: catalog__.unwrap_or_default(),
                    schema: schema__.unwrap_or_default(),
                    table: table__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.FullTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GlobalLimitExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.skip != 0 {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.GlobalLimitExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.skip != 0 {
            struct_ser.serialize_field("skip", &self.skip)?;
        }
        if self.fetch != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GlobalLimitExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "skip",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Skip,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "skip" => Ok(GeneratedField::Skip),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GlobalLimitExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.GlobalLimitExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GlobalLimitExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut skip__ = None;
                let mut fetch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Skip => {
                            if skip__.is_some() {
                                return Err(serde::de::Error::duplicate_field("skip"));
                            }
                            skip__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(GlobalLimitExecNode {
                    input: input__,
                    skip: skip__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.GlobalLimitExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for GroupingSetNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.GroupingSetNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for GroupingSetNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = GroupingSetNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.GroupingSetNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<GroupingSetNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(GroupingSetNode {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.GroupingSetNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HashJoinExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if !self.on.is_empty() {
            len += 1;
        }
        if self.join_type != 0 {
            len += 1;
        }
        if self.partition_mode != 0 {
            len += 1;
        }
        if self.null_equals_null {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        if !self.projection.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.HashJoinExecNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if !self.on.is_empty() {
            struct_ser.serialize_field("on", &self.on)?;
        }
        if self.join_type != 0 {
            let v = super::datafusion_common::JoinType::try_from(self.join_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if self.partition_mode != 0 {
            let v = PartitionMode::try_from(self.partition_mode)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.partition_mode)))?;
            struct_ser.serialize_field("partitionMode", &v)?;
        }
        if self.null_equals_null {
            struct_ser.serialize_field("nullEqualsNull", &self.null_equals_null)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        if !self.projection.is_empty() {
            struct_ser.serialize_field("projection", &self.projection)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HashJoinExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "on",
            "join_type",
            "joinType",
            "partition_mode",
            "partitionMode",
            "null_equals_null",
            "nullEqualsNull",
            "filter",
            "projection",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            On,
            JoinType,
            PartitionMode,
            NullEqualsNull,
            Filter,
            Projection,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "on" => Ok(GeneratedField::On),
                            "joinType" | "join_type" => Ok(GeneratedField::JoinType),
                            "partitionMode" | "partition_mode" => Ok(GeneratedField::PartitionMode),
                            "nullEqualsNull" | "null_equals_null" => Ok(GeneratedField::NullEqualsNull),
                            "filter" => Ok(GeneratedField::Filter),
                            "projection" => Ok(GeneratedField::Projection),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HashJoinExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.HashJoinExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<HashJoinExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut on__ = None;
                let mut join_type__ = None;
                let mut partition_mode__ = None;
                let mut null_equals_null__ = None;
                let mut filter__ = None;
                let mut projection__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                        GeneratedField::On => {
                            if on__.is_some() {
                                return Err(serde::de::Error::duplicate_field("on"));
                            }
                            on__ = Some(map_.next_value()?);
                        }
                        GeneratedField::JoinType => {
                            if join_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type__ = Some(map_.next_value::<super::datafusion_common::JoinType>()? as i32);
                        }
                        GeneratedField::PartitionMode => {
                            if partition_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionMode"));
                            }
                            partition_mode__ = Some(map_.next_value::<PartitionMode>()? as i32);
                        }
                        GeneratedField::NullEqualsNull => {
                            if null_equals_null__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullEqualsNull"));
                            }
                            null_equals_null__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(HashJoinExecNode {
                    left: left__,
                    right: right__,
                    on: on__.unwrap_or_default(),
                    join_type: join_type__.unwrap_or_default(),
                    partition_mode: partition_mode__.unwrap_or_default(),
                    null_equals_null: null_equals_null__.unwrap_or_default(),
                    filter: filter__,
                    projection: projection__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.HashJoinExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for HashRepartition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hash_expr.is_empty() {
            len += 1;
        }
        if self.partition_count != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.HashRepartition", len)?;
        if !self.hash_expr.is_empty() {
            struct_ser.serialize_field("hashExpr", &self.hash_expr)?;
        }
        if self.partition_count != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("partitionCount", ToString::to_string(&self.partition_count).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for HashRepartition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hash_expr",
            "hashExpr",
            "partition_count",
            "partitionCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HashExpr,
            PartitionCount,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hashExpr" | "hash_expr" => Ok(GeneratedField::HashExpr),
                            "partitionCount" | "partition_count" => Ok(GeneratedField::PartitionCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = HashRepartition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.HashRepartition")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<HashRepartition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hash_expr__ = None;
                let mut partition_count__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::HashExpr => {
                            if hash_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashExpr"));
                            }
                            hash_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PartitionCount => {
                            if partition_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionCount"));
                            }
                            partition_count__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(HashRepartition {
                    hash_expr: hash_expr__.unwrap_or_default(),
                    partition_count: partition_count__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.HashRepartition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ILikeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        if !self.escape_char.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ILikeNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        if !self.escape_char.is_empty() {
            struct_ser.serialize_field("escapeChar", &self.escape_char)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ILikeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "expr",
            "pattern",
            "escape_char",
            "escapeChar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            Expr,
            Pattern,
            EscapeChar,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            "escapeChar" | "escape_char" => Ok(GeneratedField::EscapeChar),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ILikeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ILikeNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ILikeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                let mut escape_char__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map_.next_value()?;
                        }
                        GeneratedField::EscapeChar => {
                            if escape_char__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escapeChar"));
                            }
                            escape_char__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ILikeNode {
                    negated: negated__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                    escape_char: escape_char__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ILikeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InListNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.list.is_empty() {
            len += 1;
        }
        if self.negated {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.InListNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.list.is_empty() {
            struct_ser.serialize_field("list", &self.list)?;
        }
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InListNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "list",
            "negated",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            List,
            Negated,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "list" => Ok(GeneratedField::List),
                            "negated" => Ok(GeneratedField::Negated),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InListNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.InListNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<InListNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut list__ = None;
                let mut negated__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::List => {
                            if list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("list"));
                            }
                            list__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(InListNode {
                    expr: expr__,
                    list: list__.unwrap_or_default(),
                    negated: negated__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.InListNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for InterleaveExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.InterleaveExecNode", len)?;
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for InterleaveExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = InterleaveExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.InterleaveExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<InterleaveExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut inputs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(InterleaveExecNode {
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.InterleaveExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsFalse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsFalse", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsFalse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsFalse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsFalse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsFalse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsFalse {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsFalse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotFalse {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotFalse", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotFalse {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotFalse;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotFalse")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsNotFalse, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsNotFalse {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotFalse", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotNull")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsNotNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsNotNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotTrue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotTrue", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotTrue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotTrue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotTrue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsNotTrue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsNotTrue {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotTrue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNotUnknown {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNotUnknown", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNotUnknown {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNotUnknown;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNotUnknown")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsNotUnknown, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsNotUnknown {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNotUnknown", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsNull")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsTrue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsTrue", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsTrue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsTrue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsTrue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsTrue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsTrue {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsTrue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for IsUnknown {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.IsUnknown", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for IsUnknown {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = IsUnknown;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.IsUnknown")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<IsUnknown, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(IsUnknown {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.IsUnknown", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinFilter {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expression.is_some() {
            len += 1;
        }
        if !self.column_indices.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JoinFilter", len)?;
        if let Some(v) = self.expression.as_ref() {
            struct_ser.serialize_field("expression", v)?;
        }
        if !self.column_indices.is_empty() {
            struct_ser.serialize_field("columnIndices", &self.column_indices)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JoinFilter {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expression",
            "column_indices",
            "columnIndices",
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expression,
            ColumnIndices,
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expression" => Ok(GeneratedField::Expression),
                            "columnIndices" | "column_indices" => Ok(GeneratedField::ColumnIndices),
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JoinFilter")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JoinFilter, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expression__ = None;
                let mut column_indices__ = None;
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expression => {
                            if expression__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expression"));
                            }
                            expression__ = map_.next_value()?;
                        }
                        GeneratedField::ColumnIndices => {
                            if column_indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnIndices"));
                            }
                            column_indices__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(JoinFilter {
                    expression: expression__,
                    column_indices: column_indices__.unwrap_or_default(),
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JoinFilter", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if self.join_type != 0 {
            len += 1;
        }
        if self.join_constraint != 0 {
            len += 1;
        }
        if !self.left_join_key.is_empty() {
            len += 1;
        }
        if !self.right_join_key.is_empty() {
            len += 1;
        }
        if self.null_equals_null {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JoinNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if self.join_type != 0 {
            let v = super::datafusion_common::JoinType::try_from(self.join_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if self.join_constraint != 0 {
            let v = super::datafusion_common::JoinConstraint::try_from(self.join_constraint)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.join_constraint)))?;
            struct_ser.serialize_field("joinConstraint", &v)?;
        }
        if !self.left_join_key.is_empty() {
            struct_ser.serialize_field("leftJoinKey", &self.left_join_key)?;
        }
        if !self.right_join_key.is_empty() {
            struct_ser.serialize_field("rightJoinKey", &self.right_join_key)?;
        }
        if self.null_equals_null {
            struct_ser.serialize_field("nullEqualsNull", &self.null_equals_null)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JoinNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "join_type",
            "joinType",
            "join_constraint",
            "joinConstraint",
            "left_join_key",
            "leftJoinKey",
            "right_join_key",
            "rightJoinKey",
            "null_equals_null",
            "nullEqualsNull",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            JoinType,
            JoinConstraint,
            LeftJoinKey,
            RightJoinKey,
            NullEqualsNull,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "joinType" | "join_type" => Ok(GeneratedField::JoinType),
                            "joinConstraint" | "join_constraint" => Ok(GeneratedField::JoinConstraint),
                            "leftJoinKey" | "left_join_key" => Ok(GeneratedField::LeftJoinKey),
                            "rightJoinKey" | "right_join_key" => Ok(GeneratedField::RightJoinKey),
                            "nullEqualsNull" | "null_equals_null" => Ok(GeneratedField::NullEqualsNull),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JoinNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JoinNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut join_type__ = None;
                let mut join_constraint__ = None;
                let mut left_join_key__ = None;
                let mut right_join_key__ = None;
                let mut null_equals_null__ = None;
                let mut filter__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                        GeneratedField::JoinType => {
                            if join_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type__ = Some(map_.next_value::<super::datafusion_common::JoinType>()? as i32);
                        }
                        GeneratedField::JoinConstraint => {
                            if join_constraint__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinConstraint"));
                            }
                            join_constraint__ = Some(map_.next_value::<super::datafusion_common::JoinConstraint>()? as i32);
                        }
                        GeneratedField::LeftJoinKey => {
                            if left_join_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftJoinKey"));
                            }
                            left_join_key__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RightJoinKey => {
                            if right_join_key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightJoinKey"));
                            }
                            right_join_key__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NullEqualsNull => {
                            if null_equals_null__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullEqualsNull"));
                            }
                            null_equals_null__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                    }
                }
                Ok(JoinNode {
                    left: left__,
                    right: right__,
                    join_type: join_type__.unwrap_or_default(),
                    join_constraint: join_constraint__.unwrap_or_default(),
                    left_join_key: left_join_key__.unwrap_or_default(),
                    right_join_key: right_join_key__.unwrap_or_default(),
                    null_equals_null: null_equals_null__.unwrap_or_default(),
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JoinNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JoinOn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JoinOn", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JoinOn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JoinOn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JoinOn")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JoinOn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                    }
                }
                Ok(JoinOn {
                    left: left__,
                    right: right__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JoinOn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JsonSink {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.config.is_some() {
            len += 1;
        }
        if self.writer_options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JsonSink", len)?;
        if let Some(v) = self.config.as_ref() {
            struct_ser.serialize_field("config", v)?;
        }
        if let Some(v) = self.writer_options.as_ref() {
            struct_ser.serialize_field("writerOptions", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JsonSink {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "config",
            "writer_options",
            "writerOptions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Config,
            WriterOptions,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "config" => Ok(GeneratedField::Config),
                            "writerOptions" | "writer_options" => Ok(GeneratedField::WriterOptions),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JsonSink;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JsonSink")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JsonSink, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut config__ = None;
                let mut writer_options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Config => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config__ = map_.next_value()?;
                        }
                        GeneratedField::WriterOptions => {
                            if writer_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("writerOptions"));
                            }
                            writer_options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(JsonSink {
                    config: config__,
                    writer_options: writer_options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JsonSink", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for JsonSinkExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.sink.is_some() {
            len += 1;
        }
        if self.sink_schema.is_some() {
            len += 1;
        }
        if self.sort_order.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.JsonSinkExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.sink.as_ref() {
            struct_ser.serialize_field("sink", v)?;
        }
        if let Some(v) = self.sink_schema.as_ref() {
            struct_ser.serialize_field("sinkSchema", v)?;
        }
        if let Some(v) = self.sort_order.as_ref() {
            struct_ser.serialize_field("sortOrder", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for JsonSinkExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "sink",
            "sink_schema",
            "sinkSchema",
            "sort_order",
            "sortOrder",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Sink,
            SinkSchema,
            SortOrder,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "sink" => Ok(GeneratedField::Sink),
                            "sinkSchema" | "sink_schema" => Ok(GeneratedField::SinkSchema),
                            "sortOrder" | "sort_order" => Ok(GeneratedField::SortOrder),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = JsonSinkExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.JsonSinkExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<JsonSinkExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut sink__ = None;
                let mut sink_schema__ = None;
                let mut sort_order__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Sink => {
                            if sink__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sink"));
                            }
                            sink__ = map_.next_value()?;
                        }
                        GeneratedField::SinkSchema => {
                            if sink_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sinkSchema"));
                            }
                            sink_schema__ = map_.next_value()?;
                        }
                        GeneratedField::SortOrder => {
                            if sort_order__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortOrder"));
                            }
                            sort_order__ = map_.next_value()?;
                        }
                    }
                }
                Ok(JsonSinkExecNode {
                    input: input__,
                    sink: sink__,
                    sink_schema: sink_schema__,
                    sort_order: sort_order__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.JsonSinkExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LikeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        if !self.escape_char.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LikeNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        if !self.escape_char.is_empty() {
            struct_ser.serialize_field("escapeChar", &self.escape_char)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LikeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "expr",
            "pattern",
            "escape_char",
            "escapeChar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            Expr,
            Pattern,
            EscapeChar,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            "escapeChar" | "escape_char" => Ok(GeneratedField::EscapeChar),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LikeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LikeNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LikeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                let mut escape_char__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map_.next_value()?;
                        }
                        GeneratedField::EscapeChar => {
                            if escape_char__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escapeChar"));
                            }
                            escape_char__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(LikeNode {
                    negated: negated__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                    escape_char: escape_char__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LikeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LimitNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.skip != 0 {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LimitNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.skip != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("skip", ToString::to_string(&self.skip).as_str())?;
        }
        if self.fetch != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LimitNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "skip",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Skip,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "skip" => Ok(GeneratedField::Skip),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LimitNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LimitNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LimitNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut skip__ = None;
                let mut fetch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Skip => {
                            if skip__.is_some() {
                                return Err(serde::de::Error::duplicate_field("skip"));
                            }
                            skip__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(LimitNode {
                    input: input__,
                    skip: skip__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LimitNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListIndex {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.key.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ListIndex", len)?;
        if let Some(v) = self.key.as_ref() {
            struct_ser.serialize_field("key", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListIndex {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "key",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Key,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "key" => Ok(GeneratedField::Key),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ListIndex")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListIndex, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut key__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Key => {
                            if key__.is_some() {
                                return Err(serde::de::Error::duplicate_field("key"));
                            }
                            key__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ListIndex {
                    key: key__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ListIndex", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListRange {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.start.is_some() {
            len += 1;
        }
        if self.stop.is_some() {
            len += 1;
        }
        if self.stride.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ListRange", len)?;
        if let Some(v) = self.start.as_ref() {
            struct_ser.serialize_field("start", v)?;
        }
        if let Some(v) = self.stop.as_ref() {
            struct_ser.serialize_field("stop", v)?;
        }
        if let Some(v) = self.stride.as_ref() {
            struct_ser.serialize_field("stride", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListRange {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "start",
            "stop",
            "stride",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Start,
            Stop,
            Stride,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "start" => Ok(GeneratedField::Start),
                            "stop" => Ok(GeneratedField::Stop),
                            "stride" => Ok(GeneratedField::Stride),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListRange;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ListRange")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListRange, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut start__ = None;
                let mut stop__ = None;
                let mut stride__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Start => {
                            if start__.is_some() {
                                return Err(serde::de::Error::duplicate_field("start"));
                            }
                            start__ = map_.next_value()?;
                        }
                        GeneratedField::Stop => {
                            if stop__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stop"));
                            }
                            stop__ = map_.next_value()?;
                        }
                        GeneratedField::Stride => {
                            if stride__.is_some() {
                                return Err(serde::de::Error::duplicate_field("stride"));
                            }
                            stride__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ListRange {
                    start: start__,
                    stop: stop__,
                    stride: stride__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ListRange", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ListingTableScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_name.is_some() {
            len += 1;
        }
        if !self.paths.is_empty() {
            len += 1;
        }
        if !self.file_extension.is_empty() {
            len += 1;
        }
        if self.projection.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if !self.filters.is_empty() {
            len += 1;
        }
        if !self.table_partition_cols.is_empty() {
            len += 1;
        }
        if self.collect_stat {
            len += 1;
        }
        if self.target_partitions != 0 {
            len += 1;
        }
        if !self.file_sort_order.is_empty() {
            len += 1;
        }
        if self.file_format_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ListingTableScanNode", len)?;
        if let Some(v) = self.table_name.as_ref() {
            struct_ser.serialize_field("tableName", v)?;
        }
        if !self.paths.is_empty() {
            struct_ser.serialize_field("paths", &self.paths)?;
        }
        if !self.file_extension.is_empty() {
            struct_ser.serialize_field("fileExtension", &self.file_extension)?;
        }
        if let Some(v) = self.projection.as_ref() {
            struct_ser.serialize_field("projection", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if !self.filters.is_empty() {
            struct_ser.serialize_field("filters", &self.filters)?;
        }
        if !self.table_partition_cols.is_empty() {
            struct_ser.serialize_field("tablePartitionCols", &self.table_partition_cols)?;
        }
        if self.collect_stat {
            struct_ser.serialize_field("collectStat", &self.collect_stat)?;
        }
        if self.target_partitions != 0 {
            struct_ser.serialize_field("targetPartitions", &self.target_partitions)?;
        }
        if !self.file_sort_order.is_empty() {
            struct_ser.serialize_field("fileSortOrder", &self.file_sort_order)?;
        }
        if let Some(v) = self.file_format_type.as_ref() {
            match v {
                listing_table_scan_node::FileFormatType::Csv(v) => {
                    struct_ser.serialize_field("csv", v)?;
                }
                listing_table_scan_node::FileFormatType::Parquet(v) => {
                    struct_ser.serialize_field("parquet", v)?;
                }
                listing_table_scan_node::FileFormatType::Avro(v) => {
                    struct_ser.serialize_field("avro", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ListingTableScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name",
            "tableName",
            "paths",
            "file_extension",
            "fileExtension",
            "projection",
            "schema",
            "filters",
            "table_partition_cols",
            "tablePartitionCols",
            "collect_stat",
            "collectStat",
            "target_partitions",
            "targetPartitions",
            "file_sort_order",
            "fileSortOrder",
            "csv",
            "parquet",
            "avro",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableName,
            Paths,
            FileExtension,
            Projection,
            Schema,
            Filters,
            TablePartitionCols,
            CollectStat,
            TargetPartitions,
            FileSortOrder,
            Csv,
            Parquet,
            Avro,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "paths" => Ok(GeneratedField::Paths),
                            "fileExtension" | "file_extension" => Ok(GeneratedField::FileExtension),
                            "projection" => Ok(GeneratedField::Projection),
                            "schema" => Ok(GeneratedField::Schema),
                            "filters" => Ok(GeneratedField::Filters),
                            "tablePartitionCols" | "table_partition_cols" => Ok(GeneratedField::TablePartitionCols),
                            "collectStat" | "collect_stat" => Ok(GeneratedField::CollectStat),
                            "targetPartitions" | "target_partitions" => Ok(GeneratedField::TargetPartitions),
                            "fileSortOrder" | "file_sort_order" => Ok(GeneratedField::FileSortOrder),
                            "csv" => Ok(GeneratedField::Csv),
                            "parquet" => Ok(GeneratedField::Parquet),
                            "avro" => Ok(GeneratedField::Avro),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ListingTableScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ListingTableScanNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ListingTableScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name__ = None;
                let mut paths__ = None;
                let mut file_extension__ = None;
                let mut projection__ = None;
                let mut schema__ = None;
                let mut filters__ = None;
                let mut table_partition_cols__ = None;
                let mut collect_stat__ = None;
                let mut target_partitions__ = None;
                let mut file_sort_order__ = None;
                let mut file_format_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = map_.next_value()?;
                        }
                        GeneratedField::Paths => {
                            if paths__.is_some() {
                                return Err(serde::de::Error::duplicate_field("paths"));
                            }
                            paths__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FileExtension => {
                            if file_extension__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileExtension"));
                            }
                            file_extension__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = map_.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Filters => {
                            if filters__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filters"));
                            }
                            filters__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TablePartitionCols => {
                            if table_partition_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tablePartitionCols"));
                            }
                            table_partition_cols__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CollectStat => {
                            if collect_stat__.is_some() {
                                return Err(serde::de::Error::duplicate_field("collectStat"));
                            }
                            collect_stat__ = Some(map_.next_value()?);
                        }
                        GeneratedField::TargetPartitions => {
                            if target_partitions__.is_some() {
                                return Err(serde::de::Error::duplicate_field("targetPartitions"));
                            }
                            target_partitions__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::FileSortOrder => {
                            if file_sort_order__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fileSortOrder"));
                            }
                            file_sort_order__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Csv => {
                            if file_format_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csv"));
                            }
                            file_format_type__ = map_.next_value::<::std::option::Option<_>>()?.map(listing_table_scan_node::FileFormatType::Csv)
;
                        }
                        GeneratedField::Parquet => {
                            if file_format_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquet"));
                            }
                            file_format_type__ = map_.next_value::<::std::option::Option<_>>()?.map(listing_table_scan_node::FileFormatType::Parquet)
;
                        }
                        GeneratedField::Avro => {
                            if file_format_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("avro"));
                            }
                            file_format_type__ = map_.next_value::<::std::option::Option<_>>()?.map(listing_table_scan_node::FileFormatType::Avro)
;
                        }
                    }
                }
                Ok(ListingTableScanNode {
                    table_name: table_name__,
                    paths: paths__.unwrap_or_default(),
                    file_extension: file_extension__.unwrap_or_default(),
                    projection: projection__,
                    schema: schema__,
                    filters: filters__.unwrap_or_default(),
                    table_partition_cols: table_partition_cols__.unwrap_or_default(),
                    collect_stat: collect_stat__.unwrap_or_default(),
                    target_partitions: target_partitions__.unwrap_or_default(),
                    file_sort_order: file_sort_order__.unwrap_or_default(),
                    file_format_type: file_format_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ListingTableScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LocalLimitExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LocalLimitExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if self.fetch != 0 {
            struct_ser.serialize_field("fetch", &self.fetch)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LocalLimitExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LocalLimitExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LocalLimitExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LocalLimitExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut fetch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(LocalLimitExecNode {
                    input: input__,
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LocalLimitExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExprList {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExprList", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExprList {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExprList;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExprList")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogicalExprList, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(LogicalExprList {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExprList", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExprNode", len)?;
        if let Some(v) = self.expr_type.as_ref() {
            match v {
                logical_expr_node::ExprType::Column(v) => {
                    struct_ser.serialize_field("column", v)?;
                }
                logical_expr_node::ExprType::Alias(v) => {
                    struct_ser.serialize_field("alias", v)?;
                }
                logical_expr_node::ExprType::Literal(v) => {
                    struct_ser.serialize_field("literal", v)?;
                }
                logical_expr_node::ExprType::BinaryExpr(v) => {
                    struct_ser.serialize_field("binaryExpr", v)?;
                }
                logical_expr_node::ExprType::AggregateExpr(v) => {
                    struct_ser.serialize_field("aggregateExpr", v)?;
                }
                logical_expr_node::ExprType::IsNullExpr(v) => {
                    struct_ser.serialize_field("isNullExpr", v)?;
                }
                logical_expr_node::ExprType::IsNotNullExpr(v) => {
                    struct_ser.serialize_field("isNotNullExpr", v)?;
                }
                logical_expr_node::ExprType::NotExpr(v) => {
                    struct_ser.serialize_field("notExpr", v)?;
                }
                logical_expr_node::ExprType::Between(v) => {
                    struct_ser.serialize_field("between", v)?;
                }
                logical_expr_node::ExprType::Case(v) => {
                    struct_ser.serialize_field("case", v)?;
                }
                logical_expr_node::ExprType::Cast(v) => {
                    struct_ser.serialize_field("cast", v)?;
                }
                logical_expr_node::ExprType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                logical_expr_node::ExprType::Negative(v) => {
                    struct_ser.serialize_field("negative", v)?;
                }
                logical_expr_node::ExprType::InList(v) => {
                    struct_ser.serialize_field("inList", v)?;
                }
                logical_expr_node::ExprType::Wildcard(v) => {
                    struct_ser.serialize_field("wildcard", v)?;
                }
                logical_expr_node::ExprType::TryCast(v) => {
                    struct_ser.serialize_field("tryCast", v)?;
                }
                logical_expr_node::ExprType::WindowExpr(v) => {
                    struct_ser.serialize_field("windowExpr", v)?;
                }
                logical_expr_node::ExprType::AggregateUdfExpr(v) => {
                    struct_ser.serialize_field("aggregateUdfExpr", v)?;
                }
                logical_expr_node::ExprType::ScalarUdfExpr(v) => {
                    struct_ser.serialize_field("scalarUdfExpr", v)?;
                }
                logical_expr_node::ExprType::GroupingSet(v) => {
                    struct_ser.serialize_field("groupingSet", v)?;
                }
                logical_expr_node::ExprType::Cube(v) => {
                    struct_ser.serialize_field("cube", v)?;
                }
                logical_expr_node::ExprType::Rollup(v) => {
                    struct_ser.serialize_field("rollup", v)?;
                }
                logical_expr_node::ExprType::IsTrue(v) => {
                    struct_ser.serialize_field("isTrue", v)?;
                }
                logical_expr_node::ExprType::IsFalse(v) => {
                    struct_ser.serialize_field("isFalse", v)?;
                }
                logical_expr_node::ExprType::IsUnknown(v) => {
                    struct_ser.serialize_field("isUnknown", v)?;
                }
                logical_expr_node::ExprType::IsNotTrue(v) => {
                    struct_ser.serialize_field("isNotTrue", v)?;
                }
                logical_expr_node::ExprType::IsNotFalse(v) => {
                    struct_ser.serialize_field("isNotFalse", v)?;
                }
                logical_expr_node::ExprType::IsNotUnknown(v) => {
                    struct_ser.serialize_field("isNotUnknown", v)?;
                }
                logical_expr_node::ExprType::Like(v) => {
                    struct_ser.serialize_field("like", v)?;
                }
                logical_expr_node::ExprType::Ilike(v) => {
                    struct_ser.serialize_field("ilike", v)?;
                }
                logical_expr_node::ExprType::SimilarTo(v) => {
                    struct_ser.serialize_field("similarTo", v)?;
                }
                logical_expr_node::ExprType::Placeholder(v) => {
                    struct_ser.serialize_field("placeholder", v)?;
                }
                logical_expr_node::ExprType::Unnest(v) => {
                    struct_ser.serialize_field("unnest", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column",
            "alias",
            "literal",
            "binary_expr",
            "binaryExpr",
            "aggregate_expr",
            "aggregateExpr",
            "is_null_expr",
            "isNullExpr",
            "is_not_null_expr",
            "isNotNullExpr",
            "not_expr",
            "notExpr",
            "between",
            "case_",
            "case",
            "cast",
            "sort",
            "negative",
            "in_list",
            "inList",
            "wildcard",
            "try_cast",
            "tryCast",
            "window_expr",
            "windowExpr",
            "aggregate_udf_expr",
            "aggregateUdfExpr",
            "scalar_udf_expr",
            "scalarUdfExpr",
            "grouping_set",
            "groupingSet",
            "cube",
            "rollup",
            "is_true",
            "isTrue",
            "is_false",
            "isFalse",
            "is_unknown",
            "isUnknown",
            "is_not_true",
            "isNotTrue",
            "is_not_false",
            "isNotFalse",
            "is_not_unknown",
            "isNotUnknown",
            "like",
            "ilike",
            "similar_to",
            "similarTo",
            "placeholder",
            "unnest",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Column,
            Alias,
            Literal,
            BinaryExpr,
            AggregateExpr,
            IsNullExpr,
            IsNotNullExpr,
            NotExpr,
            Between,
            Case,
            Cast,
            Sort,
            Negative,
            InList,
            Wildcard,
            TryCast,
            WindowExpr,
            AggregateUdfExpr,
            ScalarUdfExpr,
            GroupingSet,
            Cube,
            Rollup,
            IsTrue,
            IsFalse,
            IsUnknown,
            IsNotTrue,
            IsNotFalse,
            IsNotUnknown,
            Like,
            Ilike,
            SimilarTo,
            Placeholder,
            Unnest,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "column" => Ok(GeneratedField::Column),
                            "alias" => Ok(GeneratedField::Alias),
                            "literal" => Ok(GeneratedField::Literal),
                            "binaryExpr" | "binary_expr" => Ok(GeneratedField::BinaryExpr),
                            "aggregateExpr" | "aggregate_expr" => Ok(GeneratedField::AggregateExpr),
                            "isNullExpr" | "is_null_expr" => Ok(GeneratedField::IsNullExpr),
                            "isNotNullExpr" | "is_not_null_expr" => Ok(GeneratedField::IsNotNullExpr),
                            "notExpr" | "not_expr" => Ok(GeneratedField::NotExpr),
                            "between" => Ok(GeneratedField::Between),
                            "case" | "case_" => Ok(GeneratedField::Case),
                            "cast" => Ok(GeneratedField::Cast),
                            "sort" => Ok(GeneratedField::Sort),
                            "negative" => Ok(GeneratedField::Negative),
                            "inList" | "in_list" => Ok(GeneratedField::InList),
                            "wildcard" => Ok(GeneratedField::Wildcard),
                            "tryCast" | "try_cast" => Ok(GeneratedField::TryCast),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            "aggregateUdfExpr" | "aggregate_udf_expr" => Ok(GeneratedField::AggregateUdfExpr),
                            "scalarUdfExpr" | "scalar_udf_expr" => Ok(GeneratedField::ScalarUdfExpr),
                            "groupingSet" | "grouping_set" => Ok(GeneratedField::GroupingSet),
                            "cube" => Ok(GeneratedField::Cube),
                            "rollup" => Ok(GeneratedField::Rollup),
                            "isTrue" | "is_true" => Ok(GeneratedField::IsTrue),
                            "isFalse" | "is_false" => Ok(GeneratedField::IsFalse),
                            "isUnknown" | "is_unknown" => Ok(GeneratedField::IsUnknown),
                            "isNotTrue" | "is_not_true" => Ok(GeneratedField::IsNotTrue),
                            "isNotFalse" | "is_not_false" => Ok(GeneratedField::IsNotFalse),
                            "isNotUnknown" | "is_not_unknown" => Ok(GeneratedField::IsNotUnknown),
                            "like" => Ok(GeneratedField::Like),
                            "ilike" => Ok(GeneratedField::Ilike),
                            "similarTo" | "similar_to" => Ok(GeneratedField::SimilarTo),
                            "placeholder" => Ok(GeneratedField::Placeholder),
                            "unnest" => Ok(GeneratedField::Unnest),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogicalExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Column => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("column"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Column)
;
                        }
                        GeneratedField::Alias => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Alias)
;
                        }
                        GeneratedField::Literal => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("literal"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Literal)
;
                        }
                        GeneratedField::BinaryExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::BinaryExpr)
;
                        }
                        GeneratedField::AggregateExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::AggregateExpr)
;
                        }
                        GeneratedField::IsNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNullExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNullExpr)
;
                        }
                        GeneratedField::IsNotNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotNullExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotNullExpr)
;
                        }
                        GeneratedField::NotExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::NotExpr)
;
                        }
                        GeneratedField::Between => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("between"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Between)
;
                        }
                        GeneratedField::Case => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("case"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Case)
;
                        }
                        GeneratedField::Cast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cast"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Cast)
;
                        }
                        GeneratedField::Sort => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Sort)
;
                        }
                        GeneratedField::Negative => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negative"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Negative)
;
                        }
                        GeneratedField::InList => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inList"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::InList)
;
                        }
                        GeneratedField::Wildcard => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("wildcard"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Wildcard)
;
                        }
                        GeneratedField::TryCast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tryCast"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::TryCast)
;
                        }
                        GeneratedField::WindowExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::WindowExpr)
;
                        }
                        GeneratedField::AggregateUdfExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateUdfExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::AggregateUdfExpr)
;
                        }
                        GeneratedField::ScalarUdfExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarUdfExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::ScalarUdfExpr)
;
                        }
                        GeneratedField::GroupingSet => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("groupingSet"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::GroupingSet)
;
                        }
                        GeneratedField::Cube => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cube"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Cube)
;
                        }
                        GeneratedField::Rollup => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rollup"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Rollup)
;
                        }
                        GeneratedField::IsTrue => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isTrue"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsTrue)
;
                        }
                        GeneratedField::IsFalse => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isFalse"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsFalse)
;
                        }
                        GeneratedField::IsUnknown => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isUnknown"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsUnknown)
;
                        }
                        GeneratedField::IsNotTrue => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotTrue"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotTrue)
;
                        }
                        GeneratedField::IsNotFalse => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotFalse"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotFalse)
;
                        }
                        GeneratedField::IsNotUnknown => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotUnknown"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::IsNotUnknown)
;
                        }
                        GeneratedField::Like => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("like"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Like)
;
                        }
                        GeneratedField::Ilike => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ilike"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Ilike)
;
                        }
                        GeneratedField::SimilarTo => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("similarTo"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::SimilarTo)
;
                        }
                        GeneratedField::Placeholder => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("placeholder"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Placeholder)
;
                        }
                        GeneratedField::Unnest => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unnest"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_expr_node::ExprType::Unnest)
;
                        }
                    }
                }
                Ok(LogicalExprNode {
                    expr_type: expr_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExprNodeCollection {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.logical_expr_nodes.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExprNodeCollection", len)?;
        if !self.logical_expr_nodes.is_empty() {
            struct_ser.serialize_field("logicalExprNodes", &self.logical_expr_nodes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExprNodeCollection {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "logical_expr_nodes",
            "logicalExprNodes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            LogicalExprNodes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "logicalExprNodes" | "logical_expr_nodes" => Ok(GeneratedField::LogicalExprNodes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExprNodeCollection;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExprNodeCollection")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogicalExprNodeCollection, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut logical_expr_nodes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::LogicalExprNodes => {
                            if logical_expr_nodes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("logicalExprNodes"));
                            }
                            logical_expr_nodes__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(LogicalExprNodeCollection {
                    logical_expr_nodes: logical_expr_nodes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExprNodeCollection", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalExtensionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.node.is_empty() {
            len += 1;
        }
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalExtensionNode", len)?;
        if !self.node.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("node", pbjson::private::base64::encode(&self.node).as_str())?;
        }
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalExtensionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node",
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Node,
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "node" => Ok(GeneratedField::Node),
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalExtensionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalExtensionNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogicalExtensionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node__ = None;
                let mut inputs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Node => {
                            if node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(LogicalExtensionNode {
                    node: node__.unwrap_or_default(),
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalExtensionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LogicalPlanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.logical_plan_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.LogicalPlanNode", len)?;
        if let Some(v) = self.logical_plan_type.as_ref() {
            match v {
                logical_plan_node::LogicalPlanType::ListingScan(v) => {
                    struct_ser.serialize_field("listingScan", v)?;
                }
                logical_plan_node::LogicalPlanType::Projection(v) => {
                    struct_ser.serialize_field("projection", v)?;
                }
                logical_plan_node::LogicalPlanType::Selection(v) => {
                    struct_ser.serialize_field("selection", v)?;
                }
                logical_plan_node::LogicalPlanType::Limit(v) => {
                    struct_ser.serialize_field("limit", v)?;
                }
                logical_plan_node::LogicalPlanType::Aggregate(v) => {
                    struct_ser.serialize_field("aggregate", v)?;
                }
                logical_plan_node::LogicalPlanType::Join(v) => {
                    struct_ser.serialize_field("join", v)?;
                }
                logical_plan_node::LogicalPlanType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                logical_plan_node::LogicalPlanType::Repartition(v) => {
                    struct_ser.serialize_field("repartition", v)?;
                }
                logical_plan_node::LogicalPlanType::EmptyRelation(v) => {
                    struct_ser.serialize_field("emptyRelation", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateExternalTable(v) => {
                    struct_ser.serialize_field("createExternalTable", v)?;
                }
                logical_plan_node::LogicalPlanType::Explain(v) => {
                    struct_ser.serialize_field("explain", v)?;
                }
                logical_plan_node::LogicalPlanType::Window(v) => {
                    struct_ser.serialize_field("window", v)?;
                }
                logical_plan_node::LogicalPlanType::Analyze(v) => {
                    struct_ser.serialize_field("analyze", v)?;
                }
                logical_plan_node::LogicalPlanType::CrossJoin(v) => {
                    struct_ser.serialize_field("crossJoin", v)?;
                }
                logical_plan_node::LogicalPlanType::Values(v) => {
                    struct_ser.serialize_field("values", v)?;
                }
                logical_plan_node::LogicalPlanType::Extension(v) => {
                    struct_ser.serialize_field("extension", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateCatalogSchema(v) => {
                    struct_ser.serialize_field("createCatalogSchema", v)?;
                }
                logical_plan_node::LogicalPlanType::Union(v) => {
                    struct_ser.serialize_field("union", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateCatalog(v) => {
                    struct_ser.serialize_field("createCatalog", v)?;
                }
                logical_plan_node::LogicalPlanType::SubqueryAlias(v) => {
                    struct_ser.serialize_field("subqueryAlias", v)?;
                }
                logical_plan_node::LogicalPlanType::CreateView(v) => {
                    struct_ser.serialize_field("createView", v)?;
                }
                logical_plan_node::LogicalPlanType::Distinct(v) => {
                    struct_ser.serialize_field("distinct", v)?;
                }
                logical_plan_node::LogicalPlanType::ViewScan(v) => {
                    struct_ser.serialize_field("viewScan", v)?;
                }
                logical_plan_node::LogicalPlanType::CustomScan(v) => {
                    struct_ser.serialize_field("customScan", v)?;
                }
                logical_plan_node::LogicalPlanType::Prepare(v) => {
                    struct_ser.serialize_field("prepare", v)?;
                }
                logical_plan_node::LogicalPlanType::DropView(v) => {
                    struct_ser.serialize_field("dropView", v)?;
                }
                logical_plan_node::LogicalPlanType::DistinctOn(v) => {
                    struct_ser.serialize_field("distinctOn", v)?;
                }
                logical_plan_node::LogicalPlanType::CopyTo(v) => {
                    struct_ser.serialize_field("copyTo", v)?;
                }
                logical_plan_node::LogicalPlanType::Unnest(v) => {
                    struct_ser.serialize_field("unnest", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LogicalPlanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "listing_scan",
            "listingScan",
            "projection",
            "selection",
            "limit",
            "aggregate",
            "join",
            "sort",
            "repartition",
            "empty_relation",
            "emptyRelation",
            "create_external_table",
            "createExternalTable",
            "explain",
            "window",
            "analyze",
            "cross_join",
            "crossJoin",
            "values",
            "extension",
            "create_catalog_schema",
            "createCatalogSchema",
            "union",
            "create_catalog",
            "createCatalog",
            "subquery_alias",
            "subqueryAlias",
            "create_view",
            "createView",
            "distinct",
            "view_scan",
            "viewScan",
            "custom_scan",
            "customScan",
            "prepare",
            "drop_view",
            "dropView",
            "distinct_on",
            "distinctOn",
            "copy_to",
            "copyTo",
            "unnest",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ListingScan,
            Projection,
            Selection,
            Limit,
            Aggregate,
            Join,
            Sort,
            Repartition,
            EmptyRelation,
            CreateExternalTable,
            Explain,
            Window,
            Analyze,
            CrossJoin,
            Values,
            Extension,
            CreateCatalogSchema,
            Union,
            CreateCatalog,
            SubqueryAlias,
            CreateView,
            Distinct,
            ViewScan,
            CustomScan,
            Prepare,
            DropView,
            DistinctOn,
            CopyTo,
            Unnest,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "listingScan" | "listing_scan" => Ok(GeneratedField::ListingScan),
                            "projection" => Ok(GeneratedField::Projection),
                            "selection" => Ok(GeneratedField::Selection),
                            "limit" => Ok(GeneratedField::Limit),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "join" => Ok(GeneratedField::Join),
                            "sort" => Ok(GeneratedField::Sort),
                            "repartition" => Ok(GeneratedField::Repartition),
                            "emptyRelation" | "empty_relation" => Ok(GeneratedField::EmptyRelation),
                            "createExternalTable" | "create_external_table" => Ok(GeneratedField::CreateExternalTable),
                            "explain" => Ok(GeneratedField::Explain),
                            "window" => Ok(GeneratedField::Window),
                            "analyze" => Ok(GeneratedField::Analyze),
                            "crossJoin" | "cross_join" => Ok(GeneratedField::CrossJoin),
                            "values" => Ok(GeneratedField::Values),
                            "extension" => Ok(GeneratedField::Extension),
                            "createCatalogSchema" | "create_catalog_schema" => Ok(GeneratedField::CreateCatalogSchema),
                            "union" => Ok(GeneratedField::Union),
                            "createCatalog" | "create_catalog" => Ok(GeneratedField::CreateCatalog),
                            "subqueryAlias" | "subquery_alias" => Ok(GeneratedField::SubqueryAlias),
                            "createView" | "create_view" => Ok(GeneratedField::CreateView),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "viewScan" | "view_scan" => Ok(GeneratedField::ViewScan),
                            "customScan" | "custom_scan" => Ok(GeneratedField::CustomScan),
                            "prepare" => Ok(GeneratedField::Prepare),
                            "dropView" | "drop_view" => Ok(GeneratedField::DropView),
                            "distinctOn" | "distinct_on" => Ok(GeneratedField::DistinctOn),
                            "copyTo" | "copy_to" => Ok(GeneratedField::CopyTo),
                            "unnest" => Ok(GeneratedField::Unnest),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LogicalPlanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.LogicalPlanNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LogicalPlanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut logical_plan_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ListingScan => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listingScan"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::ListingScan)
;
                        }
                        GeneratedField::Projection => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Projection)
;
                        }
                        GeneratedField::Selection => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("selection"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Selection)
;
                        }
                        GeneratedField::Limit => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Limit)
;
                        }
                        GeneratedField::Aggregate => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Aggregate)
;
                        }
                        GeneratedField::Join => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("join"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Join)
;
                        }
                        GeneratedField::Sort => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Sort)
;
                        }
                        GeneratedField::Repartition => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("repartition"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Repartition)
;
                        }
                        GeneratedField::EmptyRelation => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("emptyRelation"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::EmptyRelation)
;
                        }
                        GeneratedField::CreateExternalTable => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createExternalTable"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateExternalTable)
;
                        }
                        GeneratedField::Explain => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("explain"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Explain)
;
                        }
                        GeneratedField::Window => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("window"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Window)
;
                        }
                        GeneratedField::Analyze => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("analyze"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Analyze)
;
                        }
                        GeneratedField::CrossJoin => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("crossJoin"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CrossJoin)
;
                        }
                        GeneratedField::Values => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Values)
;
                        }
                        GeneratedField::Extension => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Extension)
;
                        }
                        GeneratedField::CreateCatalogSchema => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createCatalogSchema"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateCatalogSchema)
;
                        }
                        GeneratedField::Union => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("union"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Union)
;
                        }
                        GeneratedField::CreateCatalog => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createCatalog"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateCatalog)
;
                        }
                        GeneratedField::SubqueryAlias => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("subqueryAlias"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::SubqueryAlias)
;
                        }
                        GeneratedField::CreateView => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("createView"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CreateView)
;
                        }
                        GeneratedField::Distinct => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Distinct)
;
                        }
                        GeneratedField::ViewScan => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("viewScan"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::ViewScan)
;
                        }
                        GeneratedField::CustomScan => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("customScan"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CustomScan)
;
                        }
                        GeneratedField::Prepare => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("prepare"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Prepare)
;
                        }
                        GeneratedField::DropView => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dropView"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::DropView)
;
                        }
                        GeneratedField::DistinctOn => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinctOn"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::DistinctOn)
;
                        }
                        GeneratedField::CopyTo => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("copyTo"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::CopyTo)
;
                        }
                        GeneratedField::Unnest => {
                            if logical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unnest"));
                            }
                            logical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(logical_plan_node::LogicalPlanType::Unnest)
;
                        }
                    }
                }
                Ok(LogicalPlanNode {
                    logical_plan_type: logical_plan_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.LogicalPlanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaybeFilter {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.MaybeFilter", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaybeFilter {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaybeFilter;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.MaybeFilter")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MaybeFilter, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(MaybeFilter {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.MaybeFilter", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for MaybePhysicalSortExprs {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.sort_expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.MaybePhysicalSortExprs", len)?;
        if !self.sort_expr.is_empty() {
            struct_ser.serialize_field("sortExpr", &self.sort_expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for MaybePhysicalSortExprs {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "sort_expr",
            "sortExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            SortExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "sortExpr" | "sort_expr" => Ok(GeneratedField::SortExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = MaybePhysicalSortExprs;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.MaybePhysicalSortExprs")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<MaybePhysicalSortExprs, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut sort_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::SortExpr => {
                            if sort_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortExpr"));
                            }
                            sort_expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(MaybePhysicalSortExprs {
                    sort_expr: sort_expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.MaybePhysicalSortExprs", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NamedStructField {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.name.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.NamedStructField", len)?;
        if let Some(v) = self.name.as_ref() {
            struct_ser.serialize_field("name", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NamedStructField {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NamedStructField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.NamedStructField")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NamedStructField, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = map_.next_value()?;
                        }
                    }
                }
                Ok(NamedStructField {
                    name: name__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.NamedStructField", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NegativeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.NegativeNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NegativeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NegativeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.NegativeNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NegativeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(NegativeNode {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.NegativeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for NestedLoopJoinExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if self.join_type != 0 {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.NestedLoopJoinExecNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if self.join_type != 0 {
            let v = super::datafusion_common::JoinType::try_from(self.join_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for NestedLoopJoinExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "join_type",
            "joinType",
            "filter",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            JoinType,
            Filter,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "joinType" | "join_type" => Ok(GeneratedField::JoinType),
                            "filter" => Ok(GeneratedField::Filter),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = NestedLoopJoinExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.NestedLoopJoinExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<NestedLoopJoinExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut join_type__ = None;
                let mut filter__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                        GeneratedField::JoinType => {
                            if join_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type__ = Some(map_.next_value::<super::datafusion_common::JoinType>()? as i32);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                    }
                }
                Ok(NestedLoopJoinExecNode {
                    left: left__,
                    right: right__,
                    join_type: join_type__.unwrap_or_default(),
                    filter: filter__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.NestedLoopJoinExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Not {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Not", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Not {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Not;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Not")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Not, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(Not {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Not", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OptimizedLogicalPlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.optimizer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.OptimizedLogicalPlanType", len)?;
        if !self.optimizer_name.is_empty() {
            struct_ser.serialize_field("optimizerName", &self.optimizer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OptimizedLogicalPlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "optimizer_name",
            "optimizerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OptimizerName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "optimizerName" | "optimizer_name" => Ok(GeneratedField::OptimizerName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OptimizedLogicalPlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.OptimizedLogicalPlanType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OptimizedLogicalPlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut optimizer_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OptimizerName => {
                            if optimizer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("optimizerName"));
                            }
                            optimizer_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(OptimizedLogicalPlanType {
                    optimizer_name: optimizer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.OptimizedLogicalPlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OptimizedPhysicalPlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.optimizer_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.OptimizedPhysicalPlanType", len)?;
        if !self.optimizer_name.is_empty() {
            struct_ser.serialize_field("optimizerName", &self.optimizer_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OptimizedPhysicalPlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "optimizer_name",
            "optimizerName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            OptimizerName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "optimizerName" | "optimizer_name" => Ok(GeneratedField::OptimizerName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OptimizedPhysicalPlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.OptimizedPhysicalPlanType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OptimizedPhysicalPlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut optimizer_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::OptimizerName => {
                            if optimizer_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("optimizerName"));
                            }
                            optimizer_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(OptimizedPhysicalPlanType {
                    optimizer_name: optimizer_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.OptimizedPhysicalPlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetScanExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.base_conf.is_some() {
            len += 1;
        }
        if self.predicate.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ParquetScanExecNode", len)?;
        if let Some(v) = self.base_conf.as_ref() {
            struct_ser.serialize_field("baseConf", v)?;
        }
        if let Some(v) = self.predicate.as_ref() {
            struct_ser.serialize_field("predicate", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetScanExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "base_conf",
            "baseConf",
            "predicate",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            BaseConf,
            Predicate,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "baseConf" | "base_conf" => Ok(GeneratedField::BaseConf),
                            "predicate" => Ok(GeneratedField::Predicate),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetScanExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ParquetScanExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetScanExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut base_conf__ = None;
                let mut predicate__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::BaseConf => {
                            if base_conf__.is_some() {
                                return Err(serde::de::Error::duplicate_field("baseConf"));
                            }
                            base_conf__ = map_.next_value()?;
                        }
                        GeneratedField::Predicate => {
                            if predicate__.is_some() {
                                return Err(serde::de::Error::duplicate_field("predicate"));
                            }
                            predicate__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ParquetScanExecNode {
                    base_conf: base_conf__,
                    predicate: predicate__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ParquetScanExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetSink {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.config.is_some() {
            len += 1;
        }
        if self.parquet_options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ParquetSink", len)?;
        if let Some(v) = self.config.as_ref() {
            struct_ser.serialize_field("config", v)?;
        }
        if let Some(v) = self.parquet_options.as_ref() {
            struct_ser.serialize_field("parquetOptions", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetSink {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "config",
            "parquet_options",
            "parquetOptions",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Config,
            ParquetOptions,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "config" => Ok(GeneratedField::Config),
                            "parquetOptions" | "parquet_options" => Ok(GeneratedField::ParquetOptions),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetSink;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ParquetSink")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetSink, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut config__ = None;
                let mut parquet_options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Config => {
                            if config__.is_some() {
                                return Err(serde::de::Error::duplicate_field("config"));
                            }
                            config__ = map_.next_value()?;
                        }
                        GeneratedField::ParquetOptions => {
                            if parquet_options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquetOptions"));
                            }
                            parquet_options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ParquetSink {
                    config: config__,
                    parquet_options: parquet_options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ParquetSink", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ParquetSinkExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.sink.is_some() {
            len += 1;
        }
        if self.sink_schema.is_some() {
            len += 1;
        }
        if self.sort_order.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ParquetSinkExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.sink.as_ref() {
            struct_ser.serialize_field("sink", v)?;
        }
        if let Some(v) = self.sink_schema.as_ref() {
            struct_ser.serialize_field("sinkSchema", v)?;
        }
        if let Some(v) = self.sort_order.as_ref() {
            struct_ser.serialize_field("sortOrder", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ParquetSinkExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "sink",
            "sink_schema",
            "sinkSchema",
            "sort_order",
            "sortOrder",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Sink,
            SinkSchema,
            SortOrder,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "sink" => Ok(GeneratedField::Sink),
                            "sinkSchema" | "sink_schema" => Ok(GeneratedField::SinkSchema),
                            "sortOrder" | "sort_order" => Ok(GeneratedField::SortOrder),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ParquetSinkExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ParquetSinkExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ParquetSinkExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut sink__ = None;
                let mut sink_schema__ = None;
                let mut sort_order__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Sink => {
                            if sink__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sink"));
                            }
                            sink__ = map_.next_value()?;
                        }
                        GeneratedField::SinkSchema => {
                            if sink_schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sinkSchema"));
                            }
                            sink_schema__ = map_.next_value()?;
                        }
                        GeneratedField::SortOrder => {
                            if sort_order__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortOrder"));
                            }
                            sort_order__ = map_.next_value()?;
                        }
                    }
                }
                Ok(ParquetSinkExecNode {
                    input: input__,
                    sink: sink__,
                    sink_schema: sink_schema__,
                    sort_order: sort_order__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ParquetSinkExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartialTableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.schema.is_empty() {
            len += 1;
        }
        if !self.table.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartialTableReference", len)?;
        if !self.schema.is_empty() {
            struct_ser.serialize_field("schema", &self.schema)?;
        }
        if !self.table.is_empty() {
            struct_ser.serialize_field("table", &self.table)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartialTableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
            "table",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
            Table,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schema" => Ok(GeneratedField::Schema),
                            "table" => Ok(GeneratedField::Table),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartialTableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartialTableReference")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PartialTableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema__ = None;
                let mut table__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Table => {
                            if table__.is_some() {
                                return Err(serde::de::Error::duplicate_field("table"));
                            }
                            table__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PartialTableReference {
                    schema: schema__.unwrap_or_default(),
                    table: table__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartialTableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartiallySortedInputOrderMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartiallySortedInputOrderMode", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartiallySortedInputOrderMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartiallySortedInputOrderMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartiallySortedInputOrderMode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PartiallySortedInputOrderMode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                    }
                }
                Ok(PartiallySortedInputOrderMode {
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartiallySortedInputOrderMode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionColumn", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionColumn")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PartitionColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PartitionColumn {
                    name: name__.unwrap_or_default(),
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionColumn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::CollectLeft => "COLLECT_LEFT",
            Self::Partitioned => "PARTITIONED",
            Self::Auto => "AUTO",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for PartitionMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "COLLECT_LEFT",
            "PARTITIONED",
            "AUTO",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "COLLECT_LEFT" => Ok(PartitionMode::CollectLeft),
                    "PARTITIONED" => Ok(PartitionMode::Partitioned),
                    "AUTO" => Ok(PartitionMode::Auto),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionStats {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.num_rows != 0 {
            len += 1;
        }
        if self.num_batches != 0 {
            len += 1;
        }
        if self.num_bytes != 0 {
            len += 1;
        }
        if !self.column_stats.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionStats", len)?;
        if self.num_rows != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("numRows", ToString::to_string(&self.num_rows).as_str())?;
        }
        if self.num_batches != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("numBatches", ToString::to_string(&self.num_batches).as_str())?;
        }
        if self.num_bytes != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("numBytes", ToString::to_string(&self.num_bytes).as_str())?;
        }
        if !self.column_stats.is_empty() {
            struct_ser.serialize_field("columnStats", &self.column_stats)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionStats {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "num_rows",
            "numRows",
            "num_batches",
            "numBatches",
            "num_bytes",
            "numBytes",
            "column_stats",
            "columnStats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NumRows,
            NumBatches,
            NumBytes,
            ColumnStats,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "numRows" | "num_rows" => Ok(GeneratedField::NumRows),
                            "numBatches" | "num_batches" => Ok(GeneratedField::NumBatches),
                            "numBytes" | "num_bytes" => Ok(GeneratedField::NumBytes),
                            "columnStats" | "column_stats" => Ok(GeneratedField::ColumnStats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionStats;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionStats")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PartitionStats, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut num_rows__ = None;
                let mut num_batches__ = None;
                let mut num_bytes__ = None;
                let mut column_stats__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NumRows => {
                            if num_rows__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numRows"));
                            }
                            num_rows__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NumBatches => {
                            if num_batches__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numBatches"));
                            }
                            num_batches__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::NumBytes => {
                            if num_bytes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("numBytes"));
                            }
                            num_bytes__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ColumnStats => {
                            if column_stats__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columnStats"));
                            }
                            column_stats__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PartitionStats {
                    num_rows: num_rows__.unwrap_or_default(),
                    num_batches: num_batches__.unwrap_or_default(),
                    num_bytes: num_bytes__.unwrap_or_default(),
                    column_stats: column_stats__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionStats", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PartitionedFile {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.path.is_empty() {
            len += 1;
        }
        if self.size != 0 {
            len += 1;
        }
        if self.last_modified_ns != 0 {
            len += 1;
        }
        if !self.partition_values.is_empty() {
            len += 1;
        }
        if self.range.is_some() {
            len += 1;
        }
        if self.statistics.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PartitionedFile", len)?;
        if !self.path.is_empty() {
            struct_ser.serialize_field("path", &self.path)?;
        }
        if self.size != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("size", ToString::to_string(&self.size).as_str())?;
        }
        if self.last_modified_ns != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("lastModifiedNs", ToString::to_string(&self.last_modified_ns).as_str())?;
        }
        if !self.partition_values.is_empty() {
            struct_ser.serialize_field("partitionValues", &self.partition_values)?;
        }
        if let Some(v) = self.range.as_ref() {
            struct_ser.serialize_field("range", v)?;
        }
        if let Some(v) = self.statistics.as_ref() {
            struct_ser.serialize_field("statistics", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PartitionedFile {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "path",
            "size",
            "last_modified_ns",
            "lastModifiedNs",
            "partition_values",
            "partitionValues",
            "range",
            "statistics",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Path,
            Size,
            LastModifiedNs,
            PartitionValues,
            Range,
            Statistics,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "path" => Ok(GeneratedField::Path),
                            "size" => Ok(GeneratedField::Size),
                            "lastModifiedNs" | "last_modified_ns" => Ok(GeneratedField::LastModifiedNs),
                            "partitionValues" | "partition_values" => Ok(GeneratedField::PartitionValues),
                            "range" => Ok(GeneratedField::Range),
                            "statistics" => Ok(GeneratedField::Statistics),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PartitionedFile;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PartitionedFile")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PartitionedFile, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut path__ = None;
                let mut size__ = None;
                let mut last_modified_ns__ = None;
                let mut partition_values__ = None;
                let mut range__ = None;
                let mut statistics__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Path => {
                            if path__.is_some() {
                                return Err(serde::de::Error::duplicate_field("path"));
                            }
                            path__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Size => {
                            if size__.is_some() {
                                return Err(serde::de::Error::duplicate_field("size"));
                            }
                            size__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::LastModifiedNs => {
                            if last_modified_ns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lastModifiedNs"));
                            }
                            last_modified_ns__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PartitionValues => {
                            if partition_values__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionValues"));
                            }
                            partition_values__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Range => {
                            if range__.is_some() {
                                return Err(serde::de::Error::duplicate_field("range"));
                            }
                            range__ = map_.next_value()?;
                        }
                        GeneratedField::Statistics => {
                            if statistics__.is_some() {
                                return Err(serde::de::Error::duplicate_field("statistics"));
                            }
                            statistics__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PartitionedFile {
                    path: path__.unwrap_or_default(),
                    size: size__.unwrap_or_default(),
                    last_modified_ns: last_modified_ns__.unwrap_or_default(),
                    partition_values: partition_values__.unwrap_or_default(),
                    range: range__,
                    statistics: statistics__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PartitionedFile", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Partitioning {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.partition_method.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Partitioning", len)?;
        if let Some(v) = self.partition_method.as_ref() {
            match v {
                partitioning::PartitionMethod::RoundRobin(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("roundRobin", ToString::to_string(&v).as_str())?;
                }
                partitioning::PartitionMethod::Hash(v) => {
                    struct_ser.serialize_field("hash", v)?;
                }
                partitioning::PartitionMethod::Unknown(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("unknown", ToString::to_string(&v).as_str())?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Partitioning {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "round_robin",
            "roundRobin",
            "hash",
            "unknown",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            RoundRobin,
            Hash,
            Unknown,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "roundRobin" | "round_robin" => Ok(GeneratedField::RoundRobin),
                            "hash" => Ok(GeneratedField::Hash),
                            "unknown" => Ok(GeneratedField::Unknown),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Partitioning;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Partitioning")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Partitioning, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut partition_method__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::RoundRobin => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("roundRobin"));
                            }
                            partition_method__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| partitioning::PartitionMethod::RoundRobin(x.0));
                        }
                        GeneratedField::Hash => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hash"));
                            }
                            partition_method__ = map_.next_value::<::std::option::Option<_>>()?.map(partitioning::PartitionMethod::Hash)
;
                        }
                        GeneratedField::Unknown => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("unknown"));
                            }
                            partition_method__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| partitioning::PartitionMethod::Unknown(x.0));
                        }
                    }
                }
                Ok(Partitioning {
                    partition_method: partition_method__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Partitioning", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalAggregateExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        if !self.ordering_req.is_empty() {
            len += 1;
        }
        if self.distinct {
            len += 1;
        }
        if self.aggregate_function.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalAggregateExprNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if !self.ordering_req.is_empty() {
            struct_ser.serialize_field("orderingReq", &self.ordering_req)?;
        }
        if self.distinct {
            struct_ser.serialize_field("distinct", &self.distinct)?;
        }
        if let Some(v) = self.aggregate_function.as_ref() {
            match v {
                physical_aggregate_expr_node::AggregateFunction::AggrFunction(v) => {
                    let v = AggregateFunction::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("aggrFunction", &v)?;
                }
                physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(v) => {
                    struct_ser.serialize_field("userDefinedAggrFunction", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalAggregateExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "ordering_req",
            "orderingReq",
            "distinct",
            "aggr_function",
            "aggrFunction",
            "user_defined_aggr_function",
            "userDefinedAggrFunction",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            OrderingReq,
            Distinct,
            AggrFunction,
            UserDefinedAggrFunction,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "orderingReq" | "ordering_req" => Ok(GeneratedField::OrderingReq),
                            "distinct" => Ok(GeneratedField::Distinct),
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "userDefinedAggrFunction" | "user_defined_aggr_function" => Ok(GeneratedField::UserDefinedAggrFunction),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalAggregateExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalAggregateExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalAggregateExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut ordering_req__ = None;
                let mut distinct__ = None;
                let mut aggregate_function__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OrderingReq => {
                            if ordering_req__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderingReq"));
                            }
                            ordering_req__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Distinct => {
                            if distinct__.is_some() {
                                return Err(serde::de::Error::duplicate_field("distinct"));
                            }
                            distinct__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AggrFunction => {
                            if aggregate_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            aggregate_function__ = map_.next_value::<::std::option::Option<AggregateFunction>>()?.map(|x| physical_aggregate_expr_node::AggregateFunction::AggrFunction(x as i32));
                        }
                        GeneratedField::UserDefinedAggrFunction => {
                            if aggregate_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("userDefinedAggrFunction"));
                            }
                            aggregate_function__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction);
                        }
                    }
                }
                Ok(PhysicalAggregateExprNode {
                    expr: expr__.unwrap_or_default(),
                    ordering_req: ordering_req__.unwrap_or_default(),
                    distinct: distinct__.unwrap_or_default(),
                    aggregate_function: aggregate_function__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalAggregateExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalAliasNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.alias.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalAliasNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.alias.is_empty() {
            struct_ser.serialize_field("alias", &self.alias)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalAliasNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalAliasNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalAliasNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalAliasNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut alias__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalAliasNode {
                    expr: expr__,
                    alias: alias__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalAliasNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalBinaryExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.l.is_some() {
            len += 1;
        }
        if self.r.is_some() {
            len += 1;
        }
        if !self.op.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalBinaryExprNode", len)?;
        if let Some(v) = self.l.as_ref() {
            struct_ser.serialize_field("l", v)?;
        }
        if let Some(v) = self.r.as_ref() {
            struct_ser.serialize_field("r", v)?;
        }
        if !self.op.is_empty() {
            struct_ser.serialize_field("op", &self.op)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalBinaryExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "l",
            "r",
            "op",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            L,
            R,
            Op,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "l" => Ok(GeneratedField::L),
                            "r" => Ok(GeneratedField::R),
                            "op" => Ok(GeneratedField::Op),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalBinaryExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalBinaryExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalBinaryExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut l__ = None;
                let mut r__ = None;
                let mut op__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::L => {
                            if l__.is_some() {
                                return Err(serde::de::Error::duplicate_field("l"));
                            }
                            l__ = map_.next_value()?;
                        }
                        GeneratedField::R => {
                            if r__.is_some() {
                                return Err(serde::de::Error::duplicate_field("r"));
                            }
                            r__ = map_.next_value()?;
                        }
                        GeneratedField::Op => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("op"));
                            }
                            op__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalBinaryExprNode {
                    l: l__,
                    r: r__,
                    op: op__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalBinaryExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalCaseNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.when_then_expr.is_empty() {
            len += 1;
        }
        if self.else_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalCaseNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.when_then_expr.is_empty() {
            struct_ser.serialize_field("whenThenExpr", &self.when_then_expr)?;
        }
        if let Some(v) = self.else_expr.as_ref() {
            struct_ser.serialize_field("elseExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalCaseNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "when_then_expr",
            "whenThenExpr",
            "else_expr",
            "elseExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            WhenThenExpr,
            ElseExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "whenThenExpr" | "when_then_expr" => Ok(GeneratedField::WhenThenExpr),
                            "elseExpr" | "else_expr" => Ok(GeneratedField::ElseExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalCaseNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalCaseNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalCaseNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut when_then_expr__ = None;
                let mut else_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::WhenThenExpr => {
                            if when_then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenThenExpr"));
                            }
                            when_then_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ElseExpr => {
                            if else_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("elseExpr"));
                            }
                            else_expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalCaseNode {
                    expr: expr__,
                    when_then_expr: when_then_expr__.unwrap_or_default(),
                    else_expr: else_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalCaseNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalCastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalCastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalCastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalCastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalCastNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalCastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalCastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalCastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalColumn {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if self.index != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalColumn", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if self.index != 0 {
            struct_ser.serialize_field("index", &self.index)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalColumn {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "index",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Index,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "index" => Ok(GeneratedField::Index),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalColumn;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalColumn")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalColumn, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut index__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Index => {
                            if index__.is_some() {
                                return Err(serde::de::Error::duplicate_field("index"));
                            }
                            index__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(PhysicalColumn {
                    name: name__.unwrap_or_default(),
                    index: index__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalColumn", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalDateTimeIntervalExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.l.is_some() {
            len += 1;
        }
        if self.r.is_some() {
            len += 1;
        }
        if !self.op.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalDateTimeIntervalExprNode", len)?;
        if let Some(v) = self.l.as_ref() {
            struct_ser.serialize_field("l", v)?;
        }
        if let Some(v) = self.r.as_ref() {
            struct_ser.serialize_field("r", v)?;
        }
        if !self.op.is_empty() {
            struct_ser.serialize_field("op", &self.op)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalDateTimeIntervalExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "l",
            "r",
            "op",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            L,
            R,
            Op,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "l" => Ok(GeneratedField::L),
                            "r" => Ok(GeneratedField::R),
                            "op" => Ok(GeneratedField::Op),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalDateTimeIntervalExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalDateTimeIntervalExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalDateTimeIntervalExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut l__ = None;
                let mut r__ = None;
                let mut op__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::L => {
                            if l__.is_some() {
                                return Err(serde::de::Error::duplicate_field("l"));
                            }
                            l__ = map_.next_value()?;
                        }
                        GeneratedField::R => {
                            if r__.is_some() {
                                return Err(serde::de::Error::duplicate_field("r"));
                            }
                            r__ = map_.next_value()?;
                        }
                        GeneratedField::Op => {
                            if op__.is_some() {
                                return Err(serde::de::Error::duplicate_field("op"));
                            }
                            op__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalDateTimeIntervalExprNode {
                    l: l__,
                    r: r__,
                    op: op__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalDateTimeIntervalExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalExprNode", len)?;
        if let Some(v) = self.expr_type.as_ref() {
            match v {
                physical_expr_node::ExprType::Column(v) => {
                    struct_ser.serialize_field("column", v)?;
                }
                physical_expr_node::ExprType::Literal(v) => {
                    struct_ser.serialize_field("literal", v)?;
                }
                physical_expr_node::ExprType::BinaryExpr(v) => {
                    struct_ser.serialize_field("binaryExpr", v)?;
                }
                physical_expr_node::ExprType::AggregateExpr(v) => {
                    struct_ser.serialize_field("aggregateExpr", v)?;
                }
                physical_expr_node::ExprType::IsNullExpr(v) => {
                    struct_ser.serialize_field("isNullExpr", v)?;
                }
                physical_expr_node::ExprType::IsNotNullExpr(v) => {
                    struct_ser.serialize_field("isNotNullExpr", v)?;
                }
                physical_expr_node::ExprType::NotExpr(v) => {
                    struct_ser.serialize_field("notExpr", v)?;
                }
                physical_expr_node::ExprType::Case(v) => {
                    struct_ser.serialize_field("case", v)?;
                }
                physical_expr_node::ExprType::Cast(v) => {
                    struct_ser.serialize_field("cast", v)?;
                }
                physical_expr_node::ExprType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                physical_expr_node::ExprType::Negative(v) => {
                    struct_ser.serialize_field("negative", v)?;
                }
                physical_expr_node::ExprType::InList(v) => {
                    struct_ser.serialize_field("inList", v)?;
                }
                physical_expr_node::ExprType::TryCast(v) => {
                    struct_ser.serialize_field("tryCast", v)?;
                }
                physical_expr_node::ExprType::WindowExpr(v) => {
                    struct_ser.serialize_field("windowExpr", v)?;
                }
                physical_expr_node::ExprType::ScalarUdf(v) => {
                    struct_ser.serialize_field("scalarUdf", v)?;
                }
                physical_expr_node::ExprType::LikeExpr(v) => {
                    struct_ser.serialize_field("likeExpr", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "column",
            "literal",
            "binary_expr",
            "binaryExpr",
            "aggregate_expr",
            "aggregateExpr",
            "is_null_expr",
            "isNullExpr",
            "is_not_null_expr",
            "isNotNullExpr",
            "not_expr",
            "notExpr",
            "case_",
            "case",
            "cast",
            "sort",
            "negative",
            "in_list",
            "inList",
            "try_cast",
            "tryCast",
            "window_expr",
            "windowExpr",
            "scalar_udf",
            "scalarUdf",
            "like_expr",
            "likeExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Column,
            Literal,
            BinaryExpr,
            AggregateExpr,
            IsNullExpr,
            IsNotNullExpr,
            NotExpr,
            Case,
            Cast,
            Sort,
            Negative,
            InList,
            TryCast,
            WindowExpr,
            ScalarUdf,
            LikeExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "column" => Ok(GeneratedField::Column),
                            "literal" => Ok(GeneratedField::Literal),
                            "binaryExpr" | "binary_expr" => Ok(GeneratedField::BinaryExpr),
                            "aggregateExpr" | "aggregate_expr" => Ok(GeneratedField::AggregateExpr),
                            "isNullExpr" | "is_null_expr" => Ok(GeneratedField::IsNullExpr),
                            "isNotNullExpr" | "is_not_null_expr" => Ok(GeneratedField::IsNotNullExpr),
                            "notExpr" | "not_expr" => Ok(GeneratedField::NotExpr),
                            "case" | "case_" => Ok(GeneratedField::Case),
                            "cast" => Ok(GeneratedField::Cast),
                            "sort" => Ok(GeneratedField::Sort),
                            "negative" => Ok(GeneratedField::Negative),
                            "inList" | "in_list" => Ok(GeneratedField::InList),
                            "tryCast" | "try_cast" => Ok(GeneratedField::TryCast),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            "scalarUdf" | "scalar_udf" => Ok(GeneratedField::ScalarUdf),
                            "likeExpr" | "like_expr" => Ok(GeneratedField::LikeExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Column => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("column"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Column)
;
                        }
                        GeneratedField::Literal => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("literal"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Literal)
;
                        }
                        GeneratedField::BinaryExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::BinaryExpr)
;
                        }
                        GeneratedField::AggregateExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregateExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::AggregateExpr)
;
                        }
                        GeneratedField::IsNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNullExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::IsNullExpr)
;
                        }
                        GeneratedField::IsNotNullExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("isNotNullExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::IsNotNullExpr)
;
                        }
                        GeneratedField::NotExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("notExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::NotExpr)
;
                        }
                        GeneratedField::Case => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("case"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Case)
;
                        }
                        GeneratedField::Cast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("cast"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Cast)
;
                        }
                        GeneratedField::Sort => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Sort)
;
                        }
                        GeneratedField::Negative => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negative"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::Negative)
;
                        }
                        GeneratedField::InList => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inList"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::InList)
;
                        }
                        GeneratedField::TryCast => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tryCast"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::TryCast)
;
                        }
                        GeneratedField::WindowExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::WindowExpr)
;
                        }
                        GeneratedField::ScalarUdf => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("scalarUdf"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::ScalarUdf)
;
                        }
                        GeneratedField::LikeExpr => {
                            if expr_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("likeExpr"));
                            }
                            expr_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_expr_node::ExprType::LikeExpr)
;
                        }
                    }
                }
                Ok(PhysicalExprNode {
                    expr_type: expr_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalExtensionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.node.is_empty() {
            len += 1;
        }
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalExtensionNode", len)?;
        if !self.node.is_empty() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("node", pbjson::private::base64::encode(&self.node).as_str())?;
        }
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalExtensionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "node",
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Node,
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "node" => Ok(GeneratedField::Node),
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalExtensionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalExtensionNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalExtensionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut node__ = None;
                let mut inputs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Node => {
                            if node__.is_some() {
                                return Err(serde::de::Error::duplicate_field("node"));
                            }
                            node__ = 
                                Some(map_.next_value::<::pbjson::private::BytesDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalExtensionNode {
                    node: node__.unwrap_or_default(),
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalExtensionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalHashRepartition {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.hash_expr.is_empty() {
            len += 1;
        }
        if self.partition_count != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalHashRepartition", len)?;
        if !self.hash_expr.is_empty() {
            struct_ser.serialize_field("hashExpr", &self.hash_expr)?;
        }
        if self.partition_count != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("partitionCount", ToString::to_string(&self.partition_count).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalHashRepartition {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "hash_expr",
            "hashExpr",
            "partition_count",
            "partitionCount",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            HashExpr,
            PartitionCount,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "hashExpr" | "hash_expr" => Ok(GeneratedField::HashExpr),
                            "partitionCount" | "partition_count" => Ok(GeneratedField::PartitionCount),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalHashRepartition;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalHashRepartition")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalHashRepartition, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut hash_expr__ = None;
                let mut partition_count__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::HashExpr => {
                            if hash_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashExpr"));
                            }
                            hash_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PartitionCount => {
                            if partition_count__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionCount"));
                            }
                            partition_count__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(PhysicalHashRepartition {
                    hash_expr: hash_expr__.unwrap_or_default(),
                    partition_count: partition_count__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalHashRepartition", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalInListNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.list.is_empty() {
            len += 1;
        }
        if self.negated {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalInListNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.list.is_empty() {
            struct_ser.serialize_field("list", &self.list)?;
        }
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalInListNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "list",
            "negated",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            List,
            Negated,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "list" => Ok(GeneratedField::List),
                            "negated" => Ok(GeneratedField::Negated),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalInListNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalInListNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalInListNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut list__ = None;
                let mut negated__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::List => {
                            if list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("list"));
                            }
                            list__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalInListNode {
                    expr: expr__,
                    list: list__.unwrap_or_default(),
                    negated: negated__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalInListNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalIsNotNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalIsNotNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalIsNotNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalIsNotNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalIsNotNull")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalIsNotNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalIsNotNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalIsNotNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalIsNull {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalIsNull", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalIsNull {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalIsNull;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalIsNull")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalIsNull, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalIsNull {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalIsNull", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalLikeExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.case_insensitive {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalLikeExprNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if self.case_insensitive {
            struct_ser.serialize_field("caseInsensitive", &self.case_insensitive)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalLikeExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "case_insensitive",
            "caseInsensitive",
            "expr",
            "pattern",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            CaseInsensitive,
            Expr,
            Pattern,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "caseInsensitive" | "case_insensitive" => Ok(GeneratedField::CaseInsensitive),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalLikeExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalLikeExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalLikeExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut case_insensitive__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CaseInsensitive => {
                            if case_insensitive__.is_some() {
                                return Err(serde::de::Error::duplicate_field("caseInsensitive"));
                            }
                            case_insensitive__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalLikeExprNode {
                    negated: negated__.unwrap_or_default(),
                    case_insensitive: case_insensitive__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalLikeExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalNegativeNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalNegativeNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalNegativeNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalNegativeNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalNegativeNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalNegativeNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalNegativeNode {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalNegativeNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalNot {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalNot", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalNot {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalNot;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalNot")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalNot, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalNot {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalNot", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalPlanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.physical_plan_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalPlanNode", len)?;
        if let Some(v) = self.physical_plan_type.as_ref() {
            match v {
                physical_plan_node::PhysicalPlanType::ParquetScan(v) => {
                    struct_ser.serialize_field("parquetScan", v)?;
                }
                physical_plan_node::PhysicalPlanType::CsvScan(v) => {
                    struct_ser.serialize_field("csvScan", v)?;
                }
                physical_plan_node::PhysicalPlanType::Empty(v) => {
                    struct_ser.serialize_field("empty", v)?;
                }
                physical_plan_node::PhysicalPlanType::Projection(v) => {
                    struct_ser.serialize_field("projection", v)?;
                }
                physical_plan_node::PhysicalPlanType::GlobalLimit(v) => {
                    struct_ser.serialize_field("globalLimit", v)?;
                }
                physical_plan_node::PhysicalPlanType::LocalLimit(v) => {
                    struct_ser.serialize_field("localLimit", v)?;
                }
                physical_plan_node::PhysicalPlanType::Aggregate(v) => {
                    struct_ser.serialize_field("aggregate", v)?;
                }
                physical_plan_node::PhysicalPlanType::HashJoin(v) => {
                    struct_ser.serialize_field("hashJoin", v)?;
                }
                physical_plan_node::PhysicalPlanType::Sort(v) => {
                    struct_ser.serialize_field("sort", v)?;
                }
                physical_plan_node::PhysicalPlanType::CoalesceBatches(v) => {
                    struct_ser.serialize_field("coalesceBatches", v)?;
                }
                physical_plan_node::PhysicalPlanType::Filter(v) => {
                    struct_ser.serialize_field("filter", v)?;
                }
                physical_plan_node::PhysicalPlanType::Merge(v) => {
                    struct_ser.serialize_field("merge", v)?;
                }
                physical_plan_node::PhysicalPlanType::Repartition(v) => {
                    struct_ser.serialize_field("repartition", v)?;
                }
                physical_plan_node::PhysicalPlanType::Window(v) => {
                    struct_ser.serialize_field("window", v)?;
                }
                physical_plan_node::PhysicalPlanType::CrossJoin(v) => {
                    struct_ser.serialize_field("crossJoin", v)?;
                }
                physical_plan_node::PhysicalPlanType::AvroScan(v) => {
                    struct_ser.serialize_field("avroScan", v)?;
                }
                physical_plan_node::PhysicalPlanType::Extension(v) => {
                    struct_ser.serialize_field("extension", v)?;
                }
                physical_plan_node::PhysicalPlanType::Union(v) => {
                    struct_ser.serialize_field("union", v)?;
                }
                physical_plan_node::PhysicalPlanType::Explain(v) => {
                    struct_ser.serialize_field("explain", v)?;
                }
                physical_plan_node::PhysicalPlanType::SortPreservingMerge(v) => {
                    struct_ser.serialize_field("sortPreservingMerge", v)?;
                }
                physical_plan_node::PhysicalPlanType::NestedLoopJoin(v) => {
                    struct_ser.serialize_field("nestedLoopJoin", v)?;
                }
                physical_plan_node::PhysicalPlanType::Analyze(v) => {
                    struct_ser.serialize_field("analyze", v)?;
                }
                physical_plan_node::PhysicalPlanType::JsonSink(v) => {
                    struct_ser.serialize_field("jsonSink", v)?;
                }
                physical_plan_node::PhysicalPlanType::SymmetricHashJoin(v) => {
                    struct_ser.serialize_field("symmetricHashJoin", v)?;
                }
                physical_plan_node::PhysicalPlanType::Interleave(v) => {
                    struct_ser.serialize_field("interleave", v)?;
                }
                physical_plan_node::PhysicalPlanType::PlaceholderRow(v) => {
                    struct_ser.serialize_field("placeholderRow", v)?;
                }
                physical_plan_node::PhysicalPlanType::CsvSink(v) => {
                    struct_ser.serialize_field("csvSink", v)?;
                }
                physical_plan_node::PhysicalPlanType::ParquetSink(v) => {
                    struct_ser.serialize_field("parquetSink", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalPlanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "parquet_scan",
            "parquetScan",
            "csv_scan",
            "csvScan",
            "empty",
            "projection",
            "global_limit",
            "globalLimit",
            "local_limit",
            "localLimit",
            "aggregate",
            "hash_join",
            "hashJoin",
            "sort",
            "coalesce_batches",
            "coalesceBatches",
            "filter",
            "merge",
            "repartition",
            "window",
            "cross_join",
            "crossJoin",
            "avro_scan",
            "avroScan",
            "extension",
            "union",
            "explain",
            "sort_preserving_merge",
            "sortPreservingMerge",
            "nested_loop_join",
            "nestedLoopJoin",
            "analyze",
            "json_sink",
            "jsonSink",
            "symmetric_hash_join",
            "symmetricHashJoin",
            "interleave",
            "placeholder_row",
            "placeholderRow",
            "csv_sink",
            "csvSink",
            "parquet_sink",
            "parquetSink",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            ParquetScan,
            CsvScan,
            Empty,
            Projection,
            GlobalLimit,
            LocalLimit,
            Aggregate,
            HashJoin,
            Sort,
            CoalesceBatches,
            Filter,
            Merge,
            Repartition,
            Window,
            CrossJoin,
            AvroScan,
            Extension,
            Union,
            Explain,
            SortPreservingMerge,
            NestedLoopJoin,
            Analyze,
            JsonSink,
            SymmetricHashJoin,
            Interleave,
            PlaceholderRow,
            CsvSink,
            ParquetSink,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "parquetScan" | "parquet_scan" => Ok(GeneratedField::ParquetScan),
                            "csvScan" | "csv_scan" => Ok(GeneratedField::CsvScan),
                            "empty" => Ok(GeneratedField::Empty),
                            "projection" => Ok(GeneratedField::Projection),
                            "globalLimit" | "global_limit" => Ok(GeneratedField::GlobalLimit),
                            "localLimit" | "local_limit" => Ok(GeneratedField::LocalLimit),
                            "aggregate" => Ok(GeneratedField::Aggregate),
                            "hashJoin" | "hash_join" => Ok(GeneratedField::HashJoin),
                            "sort" => Ok(GeneratedField::Sort),
                            "coalesceBatches" | "coalesce_batches" => Ok(GeneratedField::CoalesceBatches),
                            "filter" => Ok(GeneratedField::Filter),
                            "merge" => Ok(GeneratedField::Merge),
                            "repartition" => Ok(GeneratedField::Repartition),
                            "window" => Ok(GeneratedField::Window),
                            "crossJoin" | "cross_join" => Ok(GeneratedField::CrossJoin),
                            "avroScan" | "avro_scan" => Ok(GeneratedField::AvroScan),
                            "extension" => Ok(GeneratedField::Extension),
                            "union" => Ok(GeneratedField::Union),
                            "explain" => Ok(GeneratedField::Explain),
                            "sortPreservingMerge" | "sort_preserving_merge" => Ok(GeneratedField::SortPreservingMerge),
                            "nestedLoopJoin" | "nested_loop_join" => Ok(GeneratedField::NestedLoopJoin),
                            "analyze" => Ok(GeneratedField::Analyze),
                            "jsonSink" | "json_sink" => Ok(GeneratedField::JsonSink),
                            "symmetricHashJoin" | "symmetric_hash_join" => Ok(GeneratedField::SymmetricHashJoin),
                            "interleave" => Ok(GeneratedField::Interleave),
                            "placeholderRow" | "placeholder_row" => Ok(GeneratedField::PlaceholderRow),
                            "csvSink" | "csv_sink" => Ok(GeneratedField::CsvSink),
                            "parquetSink" | "parquet_sink" => Ok(GeneratedField::ParquetSink),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalPlanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalPlanNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalPlanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut physical_plan_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::ParquetScan => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquetScan"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::ParquetScan)
;
                        }
                        GeneratedField::CsvScan => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csvScan"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CsvScan)
;
                        }
                        GeneratedField::Empty => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("empty"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Empty)
;
                        }
                        GeneratedField::Projection => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Projection)
;
                        }
                        GeneratedField::GlobalLimit => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("globalLimit"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::GlobalLimit)
;
                        }
                        GeneratedField::LocalLimit => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("localLimit"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::LocalLimit)
;
                        }
                        GeneratedField::Aggregate => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggregate"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Aggregate)
;
                        }
                        GeneratedField::HashJoin => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hashJoin"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::HashJoin)
;
                        }
                        GeneratedField::Sort => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sort"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Sort)
;
                        }
                        GeneratedField::CoalesceBatches => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("coalesceBatches"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CoalesceBatches)
;
                        }
                        GeneratedField::Filter => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Filter)
;
                        }
                        GeneratedField::Merge => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("merge"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Merge)
;
                        }
                        GeneratedField::Repartition => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("repartition"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Repartition)
;
                        }
                        GeneratedField::Window => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("window"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Window)
;
                        }
                        GeneratedField::CrossJoin => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("crossJoin"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CrossJoin)
;
                        }
                        GeneratedField::AvroScan => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("avroScan"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::AvroScan)
;
                        }
                        GeneratedField::Extension => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("extension"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Extension)
;
                        }
                        GeneratedField::Union => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("union"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Union)
;
                        }
                        GeneratedField::Explain => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("explain"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Explain)
;
                        }
                        GeneratedField::SortPreservingMerge => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sortPreservingMerge"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::SortPreservingMerge)
;
                        }
                        GeneratedField::NestedLoopJoin => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nestedLoopJoin"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::NestedLoopJoin)
;
                        }
                        GeneratedField::Analyze => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("analyze"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Analyze)
;
                        }
                        GeneratedField::JsonSink => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("jsonSink"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::JsonSink)
;
                        }
                        GeneratedField::SymmetricHashJoin => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("symmetricHashJoin"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::SymmetricHashJoin)
;
                        }
                        GeneratedField::Interleave => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("interleave"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::Interleave)
;
                        }
                        GeneratedField::PlaceholderRow => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("placeholderRow"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::PlaceholderRow)
;
                        }
                        GeneratedField::CsvSink => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("csvSink"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::CsvSink)
;
                        }
                        GeneratedField::ParquetSink => {
                            if physical_plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("parquetSink"));
                            }
                            physical_plan_type__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_plan_node::PhysicalPlanType::ParquetSink)
;
                        }
                    }
                }
                Ok(PhysicalPlanNode {
                    physical_plan_type: physical_plan_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalPlanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalScalarUdfNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.fun_definition.is_some() {
            len += 1;
        }
        if self.return_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalScalarUdfNode", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.fun_definition.as_ref() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("funDefinition", pbjson::private::base64::encode(&v).as_str())?;
        }
        if let Some(v) = self.return_type.as_ref() {
            struct_ser.serialize_field("returnType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalScalarUdfNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "args",
            "fun_definition",
            "funDefinition",
            "return_type",
            "returnType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            Args,
            FunDefinition,
            ReturnType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "args" => Ok(GeneratedField::Args),
                            "funDefinition" | "fun_definition" => Ok(GeneratedField::FunDefinition),
                            "returnType" | "return_type" => Ok(GeneratedField::ReturnType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalScalarUdfNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalScalarUdfNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalScalarUdfNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut args__ = None;
                let mut fun_definition__ = None;
                let mut return_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FunDefinition => {
                            if fun_definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funDefinition"));
                            }
                            fun_definition__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                        GeneratedField::ReturnType => {
                            if return_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("returnType"));
                            }
                            return_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalScalarUdfNode {
                    name: name__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    fun_definition: fun_definition__,
                    return_type: return_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalScalarUdfNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalSortExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.asc {
            len += 1;
        }
        if self.nulls_first {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalSortExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.asc {
            struct_ser.serialize_field("asc", &self.asc)?;
        }
        if self.nulls_first {
            struct_ser.serialize_field("nullsFirst", &self.nulls_first)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalSortExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "asc",
            "nulls_first",
            "nullsFirst",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Asc,
            NullsFirst,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "asc" => Ok(GeneratedField::Asc),
                            "nullsFirst" | "nulls_first" => Ok(GeneratedField::NullsFirst),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalSortExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalSortExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalSortExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut asc__ = None;
                let mut nulls_first__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Asc => {
                            if asc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("asc"));
                            }
                            asc__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NullsFirst => {
                            if nulls_first__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullsFirst"));
                            }
                            nulls_first__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalSortExprNode {
                    expr: expr__,
                    asc: asc__.unwrap_or_default(),
                    nulls_first: nulls_first__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalSortExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalSortExprNodeCollection {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.physical_sort_expr_nodes.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalSortExprNodeCollection", len)?;
        if !self.physical_sort_expr_nodes.is_empty() {
            struct_ser.serialize_field("physicalSortExprNodes", &self.physical_sort_expr_nodes)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalSortExprNodeCollection {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "physical_sort_expr_nodes",
            "physicalSortExprNodes",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PhysicalSortExprNodes,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "physicalSortExprNodes" | "physical_sort_expr_nodes" => Ok(GeneratedField::PhysicalSortExprNodes),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalSortExprNodeCollection;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalSortExprNodeCollection")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalSortExprNodeCollection, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut physical_sort_expr_nodes__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PhysicalSortExprNodes => {
                            if physical_sort_expr_nodes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("physicalSortExprNodes"));
                            }
                            physical_sort_expr_nodes__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(PhysicalSortExprNodeCollection {
                    physical_sort_expr_nodes: physical_sort_expr_nodes__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalSortExprNodeCollection", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalTryCastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalTryCastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalTryCastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalTryCastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalTryCastNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalTryCastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalTryCastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalTryCastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalWhenThen {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.when_expr.is_some() {
            len += 1;
        }
        if self.then_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalWhenThen", len)?;
        if let Some(v) = self.when_expr.as_ref() {
            struct_ser.serialize_field("whenExpr", v)?;
        }
        if let Some(v) = self.then_expr.as_ref() {
            struct_ser.serialize_field("thenExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalWhenThen {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "when_expr",
            "whenExpr",
            "then_expr",
            "thenExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WhenExpr,
            ThenExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "whenExpr" | "when_expr" => Ok(GeneratedField::WhenExpr),
                            "thenExpr" | "then_expr" => Ok(GeneratedField::ThenExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalWhenThen;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalWhenThen")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalWhenThen, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut when_expr__ = None;
                let mut then_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::WhenExpr => {
                            if when_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenExpr"));
                            }
                            when_expr__ = map_.next_value()?;
                        }
                        GeneratedField::ThenExpr => {
                            if then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("thenExpr"));
                            }
                            then_expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PhysicalWhenThen {
                    when_expr: when_expr__,
                    then_expr: then_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalWhenThen", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PhysicalWindowExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.args.is_empty() {
            len += 1;
        }
        if !self.partition_by.is_empty() {
            len += 1;
        }
        if !self.order_by.is_empty() {
            len += 1;
        }
        if self.window_frame.is_some() {
            len += 1;
        }
        if !self.name.is_empty() {
            len += 1;
        }
        if self.window_function.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PhysicalWindowExprNode", len)?;
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if !self.partition_by.is_empty() {
            struct_ser.serialize_field("partitionBy", &self.partition_by)?;
        }
        if !self.order_by.is_empty() {
            struct_ser.serialize_field("orderBy", &self.order_by)?;
        }
        if let Some(v) = self.window_frame.as_ref() {
            struct_ser.serialize_field("windowFrame", v)?;
        }
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if let Some(v) = self.window_function.as_ref() {
            match v {
                physical_window_expr_node::WindowFunction::AggrFunction(v) => {
                    let v = AggregateFunction::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("aggrFunction", &v)?;
                }
                physical_window_expr_node::WindowFunction::BuiltInFunction(v) => {
                    let v = BuiltInWindowFunction::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("builtInFunction", &v)?;
                }
                physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(v) => {
                    struct_ser.serialize_field("userDefinedAggrFunction", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PhysicalWindowExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "args",
            "partition_by",
            "partitionBy",
            "order_by",
            "orderBy",
            "window_frame",
            "windowFrame",
            "name",
            "aggr_function",
            "aggrFunction",
            "built_in_function",
            "builtInFunction",
            "user_defined_aggr_function",
            "userDefinedAggrFunction",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Args,
            PartitionBy,
            OrderBy,
            WindowFrame,
            Name,
            AggrFunction,
            BuiltInFunction,
            UserDefinedAggrFunction,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "args" => Ok(GeneratedField::Args),
                            "partitionBy" | "partition_by" => Ok(GeneratedField::PartitionBy),
                            "orderBy" | "order_by" => Ok(GeneratedField::OrderBy),
                            "windowFrame" | "window_frame" => Ok(GeneratedField::WindowFrame),
                            "name" => Ok(GeneratedField::Name),
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "builtInFunction" | "built_in_function" => Ok(GeneratedField::BuiltInFunction),
                            "userDefinedAggrFunction" | "user_defined_aggr_function" => Ok(GeneratedField::UserDefinedAggrFunction),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PhysicalWindowExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PhysicalWindowExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PhysicalWindowExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut args__ = None;
                let mut partition_by__ = None;
                let mut order_by__ = None;
                let mut window_frame__ = None;
                let mut name__ = None;
                let mut window_function__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PartitionBy => {
                            if partition_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionBy"));
                            }
                            partition_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OrderBy => {
                            if order_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            order_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::WindowFrame => {
                            if window_frame__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrame"));
                            }
                            window_frame__ = map_.next_value()?;
                        }
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::AggrFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<AggregateFunction>>()?.map(|x| physical_window_expr_node::WindowFunction::AggrFunction(x as i32));
                        }
                        GeneratedField::BuiltInFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("builtInFunction"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<BuiltInWindowFunction>>()?.map(|x| physical_window_expr_node::WindowFunction::BuiltInFunction(x as i32));
                        }
                        GeneratedField::UserDefinedAggrFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("userDefinedAggrFunction"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<_>>()?.map(physical_window_expr_node::WindowFunction::UserDefinedAggrFunction);
                        }
                    }
                }
                Ok(PhysicalWindowExprNode {
                    args: args__.unwrap_or_default(),
                    partition_by: partition_by__.unwrap_or_default(),
                    order_by: order_by__.unwrap_or_default(),
                    window_frame: window_frame__,
                    name: name__.unwrap_or_default(),
                    window_function: window_function__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PhysicalWindowExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlaceholderNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.data_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PlaceholderNode", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if let Some(v) = self.data_type.as_ref() {
            struct_ser.serialize_field("dataType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlaceholderNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "data_type",
            "dataType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            DataType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "dataType" | "data_type" => Ok(GeneratedField::DataType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlaceholderNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PlaceholderNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PlaceholderNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut data_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DataType => {
                            if data_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataType"));
                            }
                            data_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PlaceholderNode {
                    id: id__.unwrap_or_default(),
                    data_type: data_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PlaceholderNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlaceholderRowExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.schema.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PlaceholderRowExecNode", len)?;
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlaceholderRowExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Schema,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "schema" => Ok(GeneratedField::Schema),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlaceholderRowExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PlaceholderRowExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PlaceholderRowExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut schema__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PlaceholderRowExecNode {
                    schema: schema__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PlaceholderRowExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PlanType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.plan_type_enum.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PlanType", len)?;
        if let Some(v) = self.plan_type_enum.as_ref() {
            match v {
                plan_type::PlanTypeEnum::InitialLogicalPlan(v) => {
                    struct_ser.serialize_field("InitialLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::AnalyzedLogicalPlan(v) => {
                    struct_ser.serialize_field("AnalyzedLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::FinalAnalyzedLogicalPlan(v) => {
                    struct_ser.serialize_field("FinalAnalyzedLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::OptimizedLogicalPlan(v) => {
                    struct_ser.serialize_field("OptimizedLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::FinalLogicalPlan(v) => {
                    struct_ser.serialize_field("FinalLogicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::InitialPhysicalPlan(v) => {
                    struct_ser.serialize_field("InitialPhysicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::InitialPhysicalPlanWithStats(v) => {
                    struct_ser.serialize_field("InitialPhysicalPlanWithStats", v)?;
                }
                plan_type::PlanTypeEnum::OptimizedPhysicalPlan(v) => {
                    struct_ser.serialize_field("OptimizedPhysicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::FinalPhysicalPlan(v) => {
                    struct_ser.serialize_field("FinalPhysicalPlan", v)?;
                }
                plan_type::PlanTypeEnum::FinalPhysicalPlanWithStats(v) => {
                    struct_ser.serialize_field("FinalPhysicalPlanWithStats", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PlanType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "InitialLogicalPlan",
            "AnalyzedLogicalPlan",
            "FinalAnalyzedLogicalPlan",
            "OptimizedLogicalPlan",
            "FinalLogicalPlan",
            "InitialPhysicalPlan",
            "InitialPhysicalPlanWithStats",
            "OptimizedPhysicalPlan",
            "FinalPhysicalPlan",
            "FinalPhysicalPlanWithStats",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            InitialLogicalPlan,
            AnalyzedLogicalPlan,
            FinalAnalyzedLogicalPlan,
            OptimizedLogicalPlan,
            FinalLogicalPlan,
            InitialPhysicalPlan,
            InitialPhysicalPlanWithStats,
            OptimizedPhysicalPlan,
            FinalPhysicalPlan,
            FinalPhysicalPlanWithStats,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "InitialLogicalPlan" => Ok(GeneratedField::InitialLogicalPlan),
                            "AnalyzedLogicalPlan" => Ok(GeneratedField::AnalyzedLogicalPlan),
                            "FinalAnalyzedLogicalPlan" => Ok(GeneratedField::FinalAnalyzedLogicalPlan),
                            "OptimizedLogicalPlan" => Ok(GeneratedField::OptimizedLogicalPlan),
                            "FinalLogicalPlan" => Ok(GeneratedField::FinalLogicalPlan),
                            "InitialPhysicalPlan" => Ok(GeneratedField::InitialPhysicalPlan),
                            "InitialPhysicalPlanWithStats" => Ok(GeneratedField::InitialPhysicalPlanWithStats),
                            "OptimizedPhysicalPlan" => Ok(GeneratedField::OptimizedPhysicalPlan),
                            "FinalPhysicalPlan" => Ok(GeneratedField::FinalPhysicalPlan),
                            "FinalPhysicalPlanWithStats" => Ok(GeneratedField::FinalPhysicalPlanWithStats),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PlanType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PlanType")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PlanType, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut plan_type_enum__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::InitialLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("InitialLogicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::InitialLogicalPlan)
;
                        }
                        GeneratedField::AnalyzedLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("AnalyzedLogicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::AnalyzedLogicalPlan)
;
                        }
                        GeneratedField::FinalAnalyzedLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FinalAnalyzedLogicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::FinalAnalyzedLogicalPlan)
;
                        }
                        GeneratedField::OptimizedLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("OptimizedLogicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::OptimizedLogicalPlan)
;
                        }
                        GeneratedField::FinalLogicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FinalLogicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::FinalLogicalPlan)
;
                        }
                        GeneratedField::InitialPhysicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("InitialPhysicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::InitialPhysicalPlan)
;
                        }
                        GeneratedField::InitialPhysicalPlanWithStats => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("InitialPhysicalPlanWithStats"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::InitialPhysicalPlanWithStats)
;
                        }
                        GeneratedField::OptimizedPhysicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("OptimizedPhysicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::OptimizedPhysicalPlan)
;
                        }
                        GeneratedField::FinalPhysicalPlan => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FinalPhysicalPlan"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::FinalPhysicalPlan)
;
                        }
                        GeneratedField::FinalPhysicalPlanWithStats => {
                            if plan_type_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("FinalPhysicalPlanWithStats"));
                            }
                            plan_type_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(plan_type::PlanTypeEnum::FinalPhysicalPlanWithStats)
;
                        }
                    }
                }
                Ok(PlanType {
                    plan_type_enum: plan_type_enum__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PlanType", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for PrepareNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.name.is_empty() {
            len += 1;
        }
        if !self.data_types.is_empty() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.PrepareNode", len)?;
        if !self.name.is_empty() {
            struct_ser.serialize_field("name", &self.name)?;
        }
        if !self.data_types.is_empty() {
            struct_ser.serialize_field("dataTypes", &self.data_types)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for PrepareNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "name",
            "data_types",
            "dataTypes",
            "input",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Name,
            DataTypes,
            Input,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "name" => Ok(GeneratedField::Name),
                            "dataTypes" | "data_types" => Ok(GeneratedField::DataTypes),
                            "input" => Ok(GeneratedField::Input),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = PrepareNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.PrepareNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<PrepareNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut name__ = None;
                let mut data_types__ = None;
                let mut input__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Name => {
                            if name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("name"));
                            }
                            name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::DataTypes => {
                            if data_types__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dataTypes"));
                            }
                            data_types__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                    }
                }
                Ok(PrepareNode {
                    name: name__.unwrap_or_default(),
                    data_types: data_types__.unwrap_or_default(),
                    input: input__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.PrepareNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectionColumns {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.columns.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ProjectionColumns", len)?;
        if !self.columns.is_empty() {
            struct_ser.serialize_field("columns", &self.columns)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectionColumns {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "columns",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Columns,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "columns" => Ok(GeneratedField::Columns),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectionColumns;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ProjectionColumns")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ProjectionColumns, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut columns__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Columns => {
                            if columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("columns"));
                            }
                            columns__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ProjectionColumns {
                    columns: columns__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ProjectionColumns", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if !self.expr_name.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ProjectionExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if !self.expr_name.is_empty() {
            struct_ser.serialize_field("exprName", &self.expr_name)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "expr_name",
            "exprName",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            ExprName,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "exprName" | "expr_name" => Ok(GeneratedField::ExprName),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ProjectionExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ProjectionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut expr_name__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ExprName => {
                            if expr_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exprName"));
                            }
                            expr_name__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ProjectionExecNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    expr_name: expr_name__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ProjectionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ProjectionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.optional_alias.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ProjectionNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if let Some(v) = self.optional_alias.as_ref() {
            match v {
                projection_node::OptionalAlias::Alias(v) => {
                    struct_ser.serialize_field("alias", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ProjectionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ProjectionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ProjectionNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ProjectionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut optional_alias__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Alias => {
                            if optional_alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            optional_alias__ = map_.next_value::<::std::option::Option<_>>()?.map(projection_node::OptionalAlias::Alias);
                        }
                    }
                }
                Ok(ProjectionNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    optional_alias: optional_alias__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ProjectionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RepartitionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.partitioning.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.RepartitionExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.partitioning.as_ref() {
            struct_ser.serialize_field("partitioning", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RepartitionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "partitioning",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Partitioning,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "partitioning" => Ok(GeneratedField::Partitioning),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RepartitionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.RepartitionExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RepartitionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut partitioning__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Partitioning => {
                            if partitioning__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitioning"));
                            }
                            partitioning__ = map_.next_value()?;
                        }
                    }
                }
                Ok(RepartitionExecNode {
                    input: input__,
                    partitioning: partitioning__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.RepartitionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RepartitionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.partition_method.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.RepartitionNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.partition_method.as_ref() {
            match v {
                repartition_node::PartitionMethod::RoundRobin(v) => {
                    #[allow(clippy::needless_borrow)]
                    struct_ser.serialize_field("roundRobin", ToString::to_string(&v).as_str())?;
                }
                repartition_node::PartitionMethod::Hash(v) => {
                    struct_ser.serialize_field("hash", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RepartitionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "round_robin",
            "roundRobin",
            "hash",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            RoundRobin,
            Hash,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "roundRobin" | "round_robin" => Ok(GeneratedField::RoundRobin),
                            "hash" => Ok(GeneratedField::Hash),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RepartitionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.RepartitionNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RepartitionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut partition_method__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::RoundRobin => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("roundRobin"));
                            }
                            partition_method__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| repartition_node::PartitionMethod::RoundRobin(x.0));
                        }
                        GeneratedField::Hash => {
                            if partition_method__.is_some() {
                                return Err(serde::de::Error::duplicate_field("hash"));
                            }
                            partition_method__ = map_.next_value::<::std::option::Option<_>>()?.map(repartition_node::PartitionMethod::Hash)
;
                        }
                    }
                }
                Ok(RepartitionNode {
                    input: input__,
                    partition_method: partition_method__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.RepartitionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for RollupNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.RollupNode", len)?;
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for RollupNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = RollupNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.RollupNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<RollupNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(RollupNode {
                    expr: expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.RollupNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScalarUdfExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.fun_name.is_empty() {
            len += 1;
        }
        if !self.args.is_empty() {
            len += 1;
        }
        if self.fun_definition.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScalarUDFExprNode", len)?;
        if !self.fun_name.is_empty() {
            struct_ser.serialize_field("funName", &self.fun_name)?;
        }
        if !self.args.is_empty() {
            struct_ser.serialize_field("args", &self.args)?;
        }
        if let Some(v) = self.fun_definition.as_ref() {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("funDefinition", pbjson::private::base64::encode(&v).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScalarUdfExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "fun_name",
            "funName",
            "args",
            "fun_definition",
            "funDefinition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            FunName,
            Args,
            FunDefinition,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "funName" | "fun_name" => Ok(GeneratedField::FunName),
                            "args" => Ok(GeneratedField::Args),
                            "funDefinition" | "fun_definition" => Ok(GeneratedField::FunDefinition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScalarUdfExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScalarUDFExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScalarUdfExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut fun_name__ = None;
                let mut args__ = None;
                let mut fun_definition__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::FunName => {
                            if fun_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funName"));
                            }
                            fun_name__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Args => {
                            if args__.is_some() {
                                return Err(serde::de::Error::duplicate_field("args"));
                            }
                            args__ = Some(map_.next_value()?);
                        }
                        GeneratedField::FunDefinition => {
                            if fun_definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("funDefinition"));
                            }
                            fun_definition__ = 
                                map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| x.0)
                            ;
                        }
                    }
                }
                Ok(ScalarUdfExprNode {
                    fun_name: fun_name__.unwrap_or_default(),
                    args: args__.unwrap_or_default(),
                    fun_definition: fun_definition__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScalarUDFExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ScanLimit {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.limit != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ScanLimit", len)?;
        if self.limit != 0 {
            struct_ser.serialize_field("limit", &self.limit)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ScanLimit {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "limit",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Limit,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "limit" => Ok(GeneratedField::Limit),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ScanLimit;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ScanLimit")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ScanLimit, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut limit__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Limit => {
                            if limit__.is_some() {
                                return Err(serde::de::Error::duplicate_field("limit"));
                            }
                            limit__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(ScanLimit {
                    limit: limit__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ScanLimit", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SelectionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SelectionExecNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SelectionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SelectionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SelectionExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SelectionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SelectionExecNode {
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SelectionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SelectionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SelectionNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SelectionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SelectionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SelectionNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SelectionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SelectionNode {
                    input: input__,
                    expr: expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SelectionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SimilarToNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.negated {
            len += 1;
        }
        if self.expr.is_some() {
            len += 1;
        }
        if self.pattern.is_some() {
            len += 1;
        }
        if !self.escape_char.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SimilarToNode", len)?;
        if self.negated {
            struct_ser.serialize_field("negated", &self.negated)?;
        }
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.pattern.as_ref() {
            struct_ser.serialize_field("pattern", v)?;
        }
        if !self.escape_char.is_empty() {
            struct_ser.serialize_field("escapeChar", &self.escape_char)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SimilarToNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "negated",
            "expr",
            "pattern",
            "escape_char",
            "escapeChar",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Negated,
            Expr,
            Pattern,
            EscapeChar,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "negated" => Ok(GeneratedField::Negated),
                            "expr" => Ok(GeneratedField::Expr),
                            "pattern" => Ok(GeneratedField::Pattern),
                            "escapeChar" | "escape_char" => Ok(GeneratedField::EscapeChar),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SimilarToNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SimilarToNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SimilarToNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut negated__ = None;
                let mut expr__ = None;
                let mut pattern__ = None;
                let mut escape_char__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Negated => {
                            if negated__.is_some() {
                                return Err(serde::de::Error::duplicate_field("negated"));
                            }
                            negated__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Pattern => {
                            if pattern__.is_some() {
                                return Err(serde::de::Error::duplicate_field("pattern"));
                            }
                            pattern__ = map_.next_value()?;
                        }
                        GeneratedField::EscapeChar => {
                            if escape_char__.is_some() {
                                return Err(serde::de::Error::duplicate_field("escapeChar"));
                            }
                            escape_char__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SimilarToNode {
                    negated: negated__.unwrap_or_default(),
                    expr: expr__,
                    pattern: pattern__,
                    escape_char: escape_char__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SimilarToNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        if self.preserve_partitioning {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.fetch != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        if self.preserve_partitioning {
            struct_ser.serialize_field("preservePartitioning", &self.preserve_partitioning)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "fetch",
            "preserve_partitioning",
            "preservePartitioning",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Fetch,
            PreservePartitioning,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "fetch" => Ok(GeneratedField::Fetch),
                            "preservePartitioning" | "preserve_partitioning" => Ok(GeneratedField::PreservePartitioning),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SortExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut fetch__ = None;
                let mut preserve_partitioning__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::PreservePartitioning => {
                            if preserve_partitioning__.is_some() {
                                return Err(serde::de::Error::duplicate_field("preservePartitioning"));
                            }
                            preserve_partitioning__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SortExecNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                    preserve_partitioning: preserve_partitioning__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.asc {
            len += 1;
        }
        if self.nulls_first {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if self.asc {
            struct_ser.serialize_field("asc", &self.asc)?;
        }
        if self.nulls_first {
            struct_ser.serialize_field("nullsFirst", &self.nulls_first)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "asc",
            "nulls_first",
            "nullsFirst",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            Asc,
            NullsFirst,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "asc" => Ok(GeneratedField::Asc),
                            "nullsFirst" | "nulls_first" => Ok(GeneratedField::NullsFirst),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SortExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut asc__ = None;
                let mut nulls_first__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::Asc => {
                            if asc__.is_some() {
                                return Err(serde::de::Error::duplicate_field("asc"));
                            }
                            asc__ = Some(map_.next_value()?);
                        }
                        GeneratedField::NullsFirst => {
                            if nulls_first__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullsFirst"));
                            }
                            nulls_first__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SortExprNode {
                    expr: expr__,
                    asc: asc__.unwrap_or_default(),
                    nulls_first: nulls_first__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.fetch != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SortNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut fetch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(SortNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SortPreservingMergeExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.expr.is_empty() {
            len += 1;
        }
        if self.fetch != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SortPreservingMergeExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.expr.is_empty() {
            struct_ser.serialize_field("expr", &self.expr)?;
        }
        if self.fetch != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("fetch", ToString::to_string(&self.fetch).as_str())?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SortPreservingMergeExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "expr",
            "fetch",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Expr,
            Fetch,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "expr" => Ok(GeneratedField::Expr),
                            "fetch" => Ok(GeneratedField::Fetch),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SortPreservingMergeExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SortPreservingMergeExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SortPreservingMergeExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut expr__ = None;
                let mut fetch__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Fetch => {
                            if fetch__.is_some() {
                                return Err(serde::de::Error::duplicate_field("fetch"));
                            }
                            fetch__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                    }
                }
                Ok(SortPreservingMergeExecNode {
                    input: input__,
                    expr: expr__.unwrap_or_default(),
                    fetch: fetch__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SortPreservingMergeExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for StreamPartitionMode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::SinglePartition => "SINGLE_PARTITION",
            Self::PartitionedExec => "PARTITIONED_EXEC",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for StreamPartitionMode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "SINGLE_PARTITION",
            "PARTITIONED_EXEC",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StreamPartitionMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "SINGLE_PARTITION" => Ok(StreamPartitionMode::SinglePartition),
                    "PARTITIONED_EXEC" => Ok(StreamPartitionMode::PartitionedExec),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for StringifiedPlan {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.plan_type.is_some() {
            len += 1;
        }
        if !self.plan.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.StringifiedPlan", len)?;
        if let Some(v) = self.plan_type.as_ref() {
            struct_ser.serialize_field("planType", v)?;
        }
        if !self.plan.is_empty() {
            struct_ser.serialize_field("plan", &self.plan)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for StringifiedPlan {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "plan_type",
            "planType",
            "plan",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PlanType,
            Plan,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "planType" | "plan_type" => Ok(GeneratedField::PlanType),
                            "plan" => Ok(GeneratedField::Plan),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = StringifiedPlan;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.StringifiedPlan")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<StringifiedPlan, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut plan_type__ = None;
                let mut plan__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PlanType => {
                            if plan_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("planType"));
                            }
                            plan_type__ = map_.next_value()?;
                        }
                        GeneratedField::Plan => {
                            if plan__.is_some() {
                                return Err(serde::de::Error::duplicate_field("plan"));
                            }
                            plan__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(StringifiedPlan {
                    plan_type: plan_type__,
                    plan: plan__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.StringifiedPlan", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SubqueryAliasNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if self.alias.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SubqueryAliasNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.alias.as_ref() {
            struct_ser.serialize_field("alias", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SubqueryAliasNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "alias",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            Alias,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "alias" => Ok(GeneratedField::Alias),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SubqueryAliasNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SubqueryAliasNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SubqueryAliasNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut alias__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Alias => {
                            if alias__.is_some() {
                                return Err(serde::de::Error::duplicate_field("alias"));
                            }
                            alias__ = map_.next_value()?;
                        }
                    }
                }
                Ok(SubqueryAliasNode {
                    input: input__,
                    alias: alias__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SubqueryAliasNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for SymmetricHashJoinExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.left.is_some() {
            len += 1;
        }
        if self.right.is_some() {
            len += 1;
        }
        if !self.on.is_empty() {
            len += 1;
        }
        if self.join_type != 0 {
            len += 1;
        }
        if self.partition_mode != 0 {
            len += 1;
        }
        if self.null_equals_null {
            len += 1;
        }
        if self.filter.is_some() {
            len += 1;
        }
        if !self.left_sort_exprs.is_empty() {
            len += 1;
        }
        if !self.right_sort_exprs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.SymmetricHashJoinExecNode", len)?;
        if let Some(v) = self.left.as_ref() {
            struct_ser.serialize_field("left", v)?;
        }
        if let Some(v) = self.right.as_ref() {
            struct_ser.serialize_field("right", v)?;
        }
        if !self.on.is_empty() {
            struct_ser.serialize_field("on", &self.on)?;
        }
        if self.join_type != 0 {
            let v = super::datafusion_common::JoinType::try_from(self.join_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.join_type)))?;
            struct_ser.serialize_field("joinType", &v)?;
        }
        if self.partition_mode != 0 {
            let v = StreamPartitionMode::try_from(self.partition_mode)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.partition_mode)))?;
            struct_ser.serialize_field("partitionMode", &v)?;
        }
        if self.null_equals_null {
            struct_ser.serialize_field("nullEqualsNull", &self.null_equals_null)?;
        }
        if let Some(v) = self.filter.as_ref() {
            struct_ser.serialize_field("filter", v)?;
        }
        if !self.left_sort_exprs.is_empty() {
            struct_ser.serialize_field("leftSortExprs", &self.left_sort_exprs)?;
        }
        if !self.right_sort_exprs.is_empty() {
            struct_ser.serialize_field("rightSortExprs", &self.right_sort_exprs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for SymmetricHashJoinExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "left",
            "right",
            "on",
            "join_type",
            "joinType",
            "partition_mode",
            "partitionMode",
            "null_equals_null",
            "nullEqualsNull",
            "filter",
            "left_sort_exprs",
            "leftSortExprs",
            "right_sort_exprs",
            "rightSortExprs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Left,
            Right,
            On,
            JoinType,
            PartitionMode,
            NullEqualsNull,
            Filter,
            LeftSortExprs,
            RightSortExprs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "left" => Ok(GeneratedField::Left),
                            "right" => Ok(GeneratedField::Right),
                            "on" => Ok(GeneratedField::On),
                            "joinType" | "join_type" => Ok(GeneratedField::JoinType),
                            "partitionMode" | "partition_mode" => Ok(GeneratedField::PartitionMode),
                            "nullEqualsNull" | "null_equals_null" => Ok(GeneratedField::NullEqualsNull),
                            "filter" => Ok(GeneratedField::Filter),
                            "leftSortExprs" | "left_sort_exprs" => Ok(GeneratedField::LeftSortExprs),
                            "rightSortExprs" | "right_sort_exprs" => Ok(GeneratedField::RightSortExprs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = SymmetricHashJoinExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.SymmetricHashJoinExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<SymmetricHashJoinExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut left__ = None;
                let mut right__ = None;
                let mut on__ = None;
                let mut join_type__ = None;
                let mut partition_mode__ = None;
                let mut null_equals_null__ = None;
                let mut filter__ = None;
                let mut left_sort_exprs__ = None;
                let mut right_sort_exprs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Left => {
                            if left__.is_some() {
                                return Err(serde::de::Error::duplicate_field("left"));
                            }
                            left__ = map_.next_value()?;
                        }
                        GeneratedField::Right => {
                            if right__.is_some() {
                                return Err(serde::de::Error::duplicate_field("right"));
                            }
                            right__ = map_.next_value()?;
                        }
                        GeneratedField::On => {
                            if on__.is_some() {
                                return Err(serde::de::Error::duplicate_field("on"));
                            }
                            on__ = Some(map_.next_value()?);
                        }
                        GeneratedField::JoinType => {
                            if join_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("joinType"));
                            }
                            join_type__ = Some(map_.next_value::<super::datafusion_common::JoinType>()? as i32);
                        }
                        GeneratedField::PartitionMode => {
                            if partition_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionMode"));
                            }
                            partition_mode__ = Some(map_.next_value::<StreamPartitionMode>()? as i32);
                        }
                        GeneratedField::NullEqualsNull => {
                            if null_equals_null__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nullEqualsNull"));
                            }
                            null_equals_null__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Filter => {
                            if filter__.is_some() {
                                return Err(serde::de::Error::duplicate_field("filter"));
                            }
                            filter__ = map_.next_value()?;
                        }
                        GeneratedField::LeftSortExprs => {
                            if left_sort_exprs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("leftSortExprs"));
                            }
                            left_sort_exprs__ = Some(map_.next_value()?);
                        }
                        GeneratedField::RightSortExprs => {
                            if right_sort_exprs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("rightSortExprs"));
                            }
                            right_sort_exprs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(SymmetricHashJoinExecNode {
                    left: left__,
                    right: right__,
                    on: on__.unwrap_or_default(),
                    join_type: join_type__.unwrap_or_default(),
                    partition_mode: partition_mode__.unwrap_or_default(),
                    null_equals_null: null_equals_null__.unwrap_or_default(),
                    filter: filter__,
                    left_sort_exprs: left_sort_exprs__.unwrap_or_default(),
                    right_sort_exprs: right_sort_exprs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.SymmetricHashJoinExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TableReference {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_reference_enum.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.TableReference", len)?;
        if let Some(v) = self.table_reference_enum.as_ref() {
            match v {
                table_reference::TableReferenceEnum::Bare(v) => {
                    struct_ser.serialize_field("bare", v)?;
                }
                table_reference::TableReferenceEnum::Partial(v) => {
                    struct_ser.serialize_field("partial", v)?;
                }
                table_reference::TableReferenceEnum::Full(v) => {
                    struct_ser.serialize_field("full", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TableReference {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "bare",
            "partial",
            "full",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Bare,
            Partial,
            Full,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "bare" => Ok(GeneratedField::Bare),
                            "partial" => Ok(GeneratedField::Partial),
                            "full" => Ok(GeneratedField::Full),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TableReference;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.TableReference")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TableReference, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_reference_enum__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Bare => {
                            if table_reference_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bare"));
                            }
                            table_reference_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(table_reference::TableReferenceEnum::Bare)
;
                        }
                        GeneratedField::Partial => {
                            if table_reference_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partial"));
                            }
                            table_reference_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(table_reference::TableReferenceEnum::Partial)
;
                        }
                        GeneratedField::Full => {
                            if table_reference_enum__.is_some() {
                                return Err(serde::de::Error::duplicate_field("full"));
                            }
                            table_reference_enum__ = map_.next_value::<::std::option::Option<_>>()?.map(table_reference::TableReferenceEnum::Full)
;
                        }
                    }
                }
                Ok(TableReference {
                    table_reference_enum: table_reference_enum__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.TableReference", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for TryCastNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if self.arrow_type.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.TryCastNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if let Some(v) = self.arrow_type.as_ref() {
            struct_ser.serialize_field("arrowType", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for TryCastNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "arrow_type",
            "arrowType",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            ArrowType,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "arrowType" | "arrow_type" => Ok(GeneratedField::ArrowType),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = TryCastNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.TryCastNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<TryCastNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut arrow_type__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::ArrowType => {
                            if arrow_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("arrowType"));
                            }
                            arrow_type__ = map_.next_value()?;
                        }
                    }
                }
                Ok(TryCastNode {
                    expr: expr__,
                    arrow_type: arrow_type__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.TryCastNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnionExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.UnionExecNode", len)?;
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnionExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.UnionExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnionExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut inputs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(UnionExecNode {
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.UnionExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnionNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.inputs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.UnionNode", len)?;
        if !self.inputs.is_empty() {
            struct_ser.serialize_field("inputs", &self.inputs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnionNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "inputs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Inputs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "inputs" => Ok(GeneratedField::Inputs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnionNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.UnionNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnionNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut inputs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Inputs => {
                            if inputs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("inputs"));
                            }
                            inputs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(UnionNode {
                    inputs: inputs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.UnionNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Unnest {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.exprs.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Unnest", len)?;
        if !self.exprs.is_empty() {
            struct_ser.serialize_field("exprs", &self.exprs)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Unnest {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "exprs",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Exprs,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "exprs" => Ok(GeneratedField::Exprs),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Unnest;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Unnest")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Unnest, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut exprs__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Exprs => {
                            if exprs__.is_some() {
                                return Err(serde::de::Error::duplicate_field("exprs"));
                            }
                            exprs__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Unnest {
                    exprs: exprs__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Unnest", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnnestNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.exec_columns.is_empty() {
            len += 1;
        }
        if !self.list_type_columns.is_empty() {
            len += 1;
        }
        if !self.struct_type_columns.is_empty() {
            len += 1;
        }
        if !self.dependency_indices.is_empty() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if self.options.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.UnnestNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.exec_columns.is_empty() {
            struct_ser.serialize_field("execColumns", &self.exec_columns)?;
        }
        if !self.list_type_columns.is_empty() {
            struct_ser.serialize_field("listTypeColumns", &self.list_type_columns.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        if !self.struct_type_columns.is_empty() {
            struct_ser.serialize_field("structTypeColumns", &self.struct_type_columns.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        if !self.dependency_indices.is_empty() {
            struct_ser.serialize_field("dependencyIndices", &self.dependency_indices.iter().map(ToString::to_string).collect::<Vec<_>>())?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if let Some(v) = self.options.as_ref() {
            struct_ser.serialize_field("options", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnnestNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "exec_columns",
            "execColumns",
            "list_type_columns",
            "listTypeColumns",
            "struct_type_columns",
            "structTypeColumns",
            "dependency_indices",
            "dependencyIndices",
            "schema",
            "options",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            ExecColumns,
            ListTypeColumns,
            StructTypeColumns,
            DependencyIndices,
            Schema,
            Options,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "execColumns" | "exec_columns" => Ok(GeneratedField::ExecColumns),
                            "listTypeColumns" | "list_type_columns" => Ok(GeneratedField::ListTypeColumns),
                            "structTypeColumns" | "struct_type_columns" => Ok(GeneratedField::StructTypeColumns),
                            "dependencyIndices" | "dependency_indices" => Ok(GeneratedField::DependencyIndices),
                            "schema" => Ok(GeneratedField::Schema),
                            "options" => Ok(GeneratedField::Options),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnnestNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.UnnestNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnnestNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut exec_columns__ = None;
                let mut list_type_columns__ = None;
                let mut struct_type_columns__ = None;
                let mut dependency_indices__ = None;
                let mut schema__ = None;
                let mut options__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::ExecColumns => {
                            if exec_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("execColumns"));
                            }
                            exec_columns__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ListTypeColumns => {
                            if list_type_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("listTypeColumns"));
                            }
                            list_type_columns__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::StructTypeColumns => {
                            if struct_type_columns__.is_some() {
                                return Err(serde::de::Error::duplicate_field("structTypeColumns"));
                            }
                            struct_type_columns__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::DependencyIndices => {
                            if dependency_indices__.is_some() {
                                return Err(serde::de::Error::duplicate_field("dependencyIndices"));
                            }
                            dependency_indices__ = 
                                Some(map_.next_value::<Vec<::pbjson::private::NumberDeserialize<_>>>()?
                                    .into_iter().map(|x| x.0).collect())
                            ;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Options => {
                            if options__.is_some() {
                                return Err(serde::de::Error::duplicate_field("options"));
                            }
                            options__ = map_.next_value()?;
                        }
                    }
                }
                Ok(UnnestNode {
                    input: input__,
                    exec_columns: exec_columns__.unwrap_or_default(),
                    list_type_columns: list_type_columns__.unwrap_or_default(),
                    struct_type_columns: struct_type_columns__.unwrap_or_default(),
                    dependency_indices: dependency_indices__.unwrap_or_default(),
                    schema: schema__,
                    options: options__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.UnnestNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for UnnestOptions {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.preserve_nulls {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.UnnestOptions", len)?;
        if self.preserve_nulls {
            struct_ser.serialize_field("preserveNulls", &self.preserve_nulls)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for UnnestOptions {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "preserve_nulls",
            "preserveNulls",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            PreserveNulls,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "preserveNulls" | "preserve_nulls" => Ok(GeneratedField::PreserveNulls),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = UnnestOptions;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.UnnestOptions")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<UnnestOptions, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut preserve_nulls__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::PreserveNulls => {
                            if preserve_nulls__.is_some() {
                                return Err(serde::de::Error::duplicate_field("preserveNulls"));
                            }
                            preserve_nulls__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(UnnestOptions {
                    preserve_nulls: preserve_nulls__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.UnnestOptions", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ValuesNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.n_cols != 0 {
            len += 1;
        }
        if !self.values_list.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ValuesNode", len)?;
        if self.n_cols != 0 {
            #[allow(clippy::needless_borrow)]
            struct_ser.serialize_field("nCols", ToString::to_string(&self.n_cols).as_str())?;
        }
        if !self.values_list.is_empty() {
            struct_ser.serialize_field("valuesList", &self.values_list)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ValuesNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "n_cols",
            "nCols",
            "values_list",
            "valuesList",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            NCols,
            ValuesList,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "nCols" | "n_cols" => Ok(GeneratedField::NCols),
                            "valuesList" | "values_list" => Ok(GeneratedField::ValuesList),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ValuesNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ValuesNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ValuesNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut n_cols__ = None;
                let mut values_list__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::NCols => {
                            if n_cols__.is_some() {
                                return Err(serde::de::Error::duplicate_field("nCols"));
                            }
                            n_cols__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::ValuesList => {
                            if values_list__.is_some() {
                                return Err(serde::de::Error::duplicate_field("valuesList"));
                            }
                            values_list__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ValuesNode {
                    n_cols: n_cols__.unwrap_or_default(),
                    values_list: values_list__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ValuesNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for ViewTableScanNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.table_name.is_some() {
            len += 1;
        }
        if self.input.is_some() {
            len += 1;
        }
        if self.schema.is_some() {
            len += 1;
        }
        if self.projection.is_some() {
            len += 1;
        }
        if !self.definition.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.ViewTableScanNode", len)?;
        if let Some(v) = self.table_name.as_ref() {
            struct_ser.serialize_field("tableName", v)?;
        }
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if let Some(v) = self.schema.as_ref() {
            struct_ser.serialize_field("schema", v)?;
        }
        if let Some(v) = self.projection.as_ref() {
            struct_ser.serialize_field("projection", v)?;
        }
        if !self.definition.is_empty() {
            struct_ser.serialize_field("definition", &self.definition)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for ViewTableScanNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "table_name",
            "tableName",
            "input",
            "schema",
            "projection",
            "definition",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            TableName,
            Input,
            Schema,
            Projection,
            Definition,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "tableName" | "table_name" => Ok(GeneratedField::TableName),
                            "input" => Ok(GeneratedField::Input),
                            "schema" => Ok(GeneratedField::Schema),
                            "projection" => Ok(GeneratedField::Projection),
                            "definition" => Ok(GeneratedField::Definition),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = ViewTableScanNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.ViewTableScanNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<ViewTableScanNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut table_name__ = None;
                let mut input__ = None;
                let mut schema__ = None;
                let mut projection__ = None;
                let mut definition__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::TableName => {
                            if table_name__.is_some() {
                                return Err(serde::de::Error::duplicate_field("tableName"));
                            }
                            table_name__ = map_.next_value()?;
                        }
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::Schema => {
                            if schema__.is_some() {
                                return Err(serde::de::Error::duplicate_field("schema"));
                            }
                            schema__ = map_.next_value()?;
                        }
                        GeneratedField::Projection => {
                            if projection__.is_some() {
                                return Err(serde::de::Error::duplicate_field("projection"));
                            }
                            projection__ = map_.next_value()?;
                        }
                        GeneratedField::Definition => {
                            if definition__.is_some() {
                                return Err(serde::de::Error::duplicate_field("definition"));
                            }
                            definition__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(ViewTableScanNode {
                    table_name: table_name__,
                    input: input__,
                    schema: schema__,
                    projection: projection__,
                    definition: definition__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.ViewTableScanNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WhenThen {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.when_expr.is_some() {
            len += 1;
        }
        if self.then_expr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WhenThen", len)?;
        if let Some(v) = self.when_expr.as_ref() {
            struct_ser.serialize_field("whenExpr", v)?;
        }
        if let Some(v) = self.then_expr.as_ref() {
            struct_ser.serialize_field("thenExpr", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WhenThen {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "when_expr",
            "whenExpr",
            "then_expr",
            "thenExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WhenExpr,
            ThenExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "whenExpr" | "when_expr" => Ok(GeneratedField::WhenExpr),
                            "thenExpr" | "then_expr" => Ok(GeneratedField::ThenExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WhenThen;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WhenThen")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WhenThen, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut when_expr__ = None;
                let mut then_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::WhenExpr => {
                            if when_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("whenExpr"));
                            }
                            when_expr__ = map_.next_value()?;
                        }
                        GeneratedField::ThenExpr => {
                            if then_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("thenExpr"));
                            }
                            then_expr__ = map_.next_value()?;
                        }
                    }
                }
                Ok(WhenThen {
                    when_expr: when_expr__,
                    then_expr: then_expr__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WhenThen", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Wildcard {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.qualifier.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.Wildcard", len)?;
        if !self.qualifier.is_empty() {
            struct_ser.serialize_field("qualifier", &self.qualifier)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Wildcard {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "qualifier",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Qualifier,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "qualifier" => Ok(GeneratedField::Qualifier),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Wildcard;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.Wildcard")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Wildcard, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut qualifier__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Qualifier => {
                            if qualifier__.is_some() {
                                return Err(serde::de::Error::duplicate_field("qualifier"));
                            }
                            qualifier__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(Wildcard {
                    qualifier: qualifier__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.Wildcard", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowAggExecNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.window_expr.is_empty() {
            len += 1;
        }
        if !self.partition_keys.is_empty() {
            len += 1;
        }
        if self.input_order_mode.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowAggExecNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.window_expr.is_empty() {
            struct_ser.serialize_field("windowExpr", &self.window_expr)?;
        }
        if !self.partition_keys.is_empty() {
            struct_ser.serialize_field("partitionKeys", &self.partition_keys)?;
        }
        if let Some(v) = self.input_order_mode.as_ref() {
            match v {
                window_agg_exec_node::InputOrderMode::Linear(v) => {
                    struct_ser.serialize_field("linear", v)?;
                }
                window_agg_exec_node::InputOrderMode::PartiallySorted(v) => {
                    struct_ser.serialize_field("partiallySorted", v)?;
                }
                window_agg_exec_node::InputOrderMode::Sorted(v) => {
                    struct_ser.serialize_field("sorted", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowAggExecNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "window_expr",
            "windowExpr",
            "partition_keys",
            "partitionKeys",
            "linear",
            "partially_sorted",
            "partiallySorted",
            "sorted",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            WindowExpr,
            PartitionKeys,
            Linear,
            PartiallySorted,
            Sorted,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            "partitionKeys" | "partition_keys" => Ok(GeneratedField::PartitionKeys),
                            "linear" => Ok(GeneratedField::Linear),
                            "partiallySorted" | "partially_sorted" => Ok(GeneratedField::PartiallySorted),
                            "sorted" => Ok(GeneratedField::Sorted),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowAggExecNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowAggExecNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowAggExecNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut window_expr__ = None;
                let mut partition_keys__ = None;
                let mut input_order_mode__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::WindowExpr => {
                            if window_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            window_expr__ = Some(map_.next_value()?);
                        }
                        GeneratedField::PartitionKeys => {
                            if partition_keys__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionKeys"));
                            }
                            partition_keys__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Linear => {
                            if input_order_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("linear"));
                            }
                            input_order_mode__ = map_.next_value::<::std::option::Option<_>>()?.map(window_agg_exec_node::InputOrderMode::Linear)
;
                        }
                        GeneratedField::PartiallySorted => {
                            if input_order_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partiallySorted"));
                            }
                            input_order_mode__ = map_.next_value::<::std::option::Option<_>>()?.map(window_agg_exec_node::InputOrderMode::PartiallySorted)
;
                        }
                        GeneratedField::Sorted => {
                            if input_order_mode__.is_some() {
                                return Err(serde::de::Error::duplicate_field("sorted"));
                            }
                            input_order_mode__ = map_.next_value::<::std::option::Option<_>>()?.map(window_agg_exec_node::InputOrderMode::Sorted)
;
                        }
                    }
                }
                Ok(WindowAggExecNode {
                    input: input__,
                    window_expr: window_expr__.unwrap_or_default(),
                    partition_keys: partition_keys__.unwrap_or_default(),
                    input_order_mode: input_order_mode__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowAggExecNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowExprNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.expr.is_some() {
            len += 1;
        }
        if !self.partition_by.is_empty() {
            len += 1;
        }
        if !self.order_by.is_empty() {
            len += 1;
        }
        if self.window_frame.is_some() {
            len += 1;
        }
        if self.window_function.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowExprNode", len)?;
        if let Some(v) = self.expr.as_ref() {
            struct_ser.serialize_field("expr", v)?;
        }
        if !self.partition_by.is_empty() {
            struct_ser.serialize_field("partitionBy", &self.partition_by)?;
        }
        if !self.order_by.is_empty() {
            struct_ser.serialize_field("orderBy", &self.order_by)?;
        }
        if let Some(v) = self.window_frame.as_ref() {
            struct_ser.serialize_field("windowFrame", v)?;
        }
        if let Some(v) = self.window_function.as_ref() {
            match v {
                window_expr_node::WindowFunction::AggrFunction(v) => {
                    let v = AggregateFunction::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("aggrFunction", &v)?;
                }
                window_expr_node::WindowFunction::BuiltInFunction(v) => {
                    let v = BuiltInWindowFunction::try_from(*v)
                        .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", *v)))?;
                    struct_ser.serialize_field("builtInFunction", &v)?;
                }
                window_expr_node::WindowFunction::Udaf(v) => {
                    struct_ser.serialize_field("udaf", v)?;
                }
                window_expr_node::WindowFunction::Udwf(v) => {
                    struct_ser.serialize_field("udwf", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowExprNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "expr",
            "partition_by",
            "partitionBy",
            "order_by",
            "orderBy",
            "window_frame",
            "windowFrame",
            "aggr_function",
            "aggrFunction",
            "built_in_function",
            "builtInFunction",
            "udaf",
            "udwf",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Expr,
            PartitionBy,
            OrderBy,
            WindowFrame,
            AggrFunction,
            BuiltInFunction,
            Udaf,
            Udwf,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "expr" => Ok(GeneratedField::Expr),
                            "partitionBy" | "partition_by" => Ok(GeneratedField::PartitionBy),
                            "orderBy" | "order_by" => Ok(GeneratedField::OrderBy),
                            "windowFrame" | "window_frame" => Ok(GeneratedField::WindowFrame),
                            "aggrFunction" | "aggr_function" => Ok(GeneratedField::AggrFunction),
                            "builtInFunction" | "built_in_function" => Ok(GeneratedField::BuiltInFunction),
                            "udaf" => Ok(GeneratedField::Udaf),
                            "udwf" => Ok(GeneratedField::Udwf),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowExprNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowExprNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowExprNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut expr__ = None;
                let mut partition_by__ = None;
                let mut order_by__ = None;
                let mut window_frame__ = None;
                let mut window_function__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Expr => {
                            if expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("expr"));
                            }
                            expr__ = map_.next_value()?;
                        }
                        GeneratedField::PartitionBy => {
                            if partition_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("partitionBy"));
                            }
                            partition_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::OrderBy => {
                            if order_by__.is_some() {
                                return Err(serde::de::Error::duplicate_field("orderBy"));
                            }
                            order_by__ = Some(map_.next_value()?);
                        }
                        GeneratedField::WindowFrame => {
                            if window_frame__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrame"));
                            }
                            window_frame__ = map_.next_value()?;
                        }
                        GeneratedField::AggrFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("aggrFunction"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<AggregateFunction>>()?.map(|x| window_expr_node::WindowFunction::AggrFunction(x as i32));
                        }
                        GeneratedField::BuiltInFunction => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("builtInFunction"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<BuiltInWindowFunction>>()?.map(|x| window_expr_node::WindowFunction::BuiltInFunction(x as i32));
                        }
                        GeneratedField::Udaf => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udaf"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<_>>()?.map(window_expr_node::WindowFunction::Udaf);
                        }
                        GeneratedField::Udwf => {
                            if window_function__.is_some() {
                                return Err(serde::de::Error::duplicate_field("udwf"));
                            }
                            window_function__ = map_.next_value::<::std::option::Option<_>>()?.map(window_expr_node::WindowFunction::Udwf);
                        }
                    }
                }
                Ok(WindowExprNode {
                    expr: expr__,
                    partition_by: partition_by__.unwrap_or_default(),
                    order_by: order_by__.unwrap_or_default(),
                    window_frame: window_frame__,
                    window_function: window_function__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowExprNode", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrame {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.window_frame_units != 0 {
            len += 1;
        }
        if self.start_bound.is_some() {
            len += 1;
        }
        if self.end_bound.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowFrame", len)?;
        if self.window_frame_units != 0 {
            let v = WindowFrameUnits::try_from(self.window_frame_units)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.window_frame_units)))?;
            struct_ser.serialize_field("windowFrameUnits", &v)?;
        }
        if let Some(v) = self.start_bound.as_ref() {
            struct_ser.serialize_field("startBound", v)?;
        }
        if let Some(v) = self.end_bound.as_ref() {
            match v {
                window_frame::EndBound::Bound(v) => {
                    struct_ser.serialize_field("bound", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrame {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "window_frame_units",
            "windowFrameUnits",
            "start_bound",
            "startBound",
            "bound",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WindowFrameUnits,
            StartBound,
            Bound,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "windowFrameUnits" | "window_frame_units" => Ok(GeneratedField::WindowFrameUnits),
                            "startBound" | "start_bound" => Ok(GeneratedField::StartBound),
                            "bound" => Ok(GeneratedField::Bound),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrame;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowFrame")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowFrame, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut window_frame_units__ = None;
                let mut start_bound__ = None;
                let mut end_bound__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::WindowFrameUnits => {
                            if window_frame_units__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrameUnits"));
                            }
                            window_frame_units__ = Some(map_.next_value::<WindowFrameUnits>()? as i32);
                        }
                        GeneratedField::StartBound => {
                            if start_bound__.is_some() {
                                return Err(serde::de::Error::duplicate_field("startBound"));
                            }
                            start_bound__ = map_.next_value()?;
                        }
                        GeneratedField::Bound => {
                            if end_bound__.is_some() {
                                return Err(serde::de::Error::duplicate_field("bound"));
                            }
                            end_bound__ = map_.next_value::<::std::option::Option<_>>()?.map(window_frame::EndBound::Bound)
;
                        }
                    }
                }
                Ok(WindowFrame {
                    window_frame_units: window_frame_units__.unwrap_or_default(),
                    start_bound: start_bound__,
                    end_bound: end_bound__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowFrame", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrameBound {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.window_frame_bound_type != 0 {
            len += 1;
        }
        if self.bound_value.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowFrameBound", len)?;
        if self.window_frame_bound_type != 0 {
            let v = WindowFrameBoundType::try_from(self.window_frame_bound_type)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.window_frame_bound_type)))?;
            struct_ser.serialize_field("windowFrameBoundType", &v)?;
        }
        if let Some(v) = self.bound_value.as_ref() {
            struct_ser.serialize_field("boundValue", v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrameBound {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "window_frame_bound_type",
            "windowFrameBoundType",
            "bound_value",
            "boundValue",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            WindowFrameBoundType,
            BoundValue,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "windowFrameBoundType" | "window_frame_bound_type" => Ok(GeneratedField::WindowFrameBoundType),
                            "boundValue" | "bound_value" => Ok(GeneratedField::BoundValue),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrameBound;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowFrameBound")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowFrameBound, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut window_frame_bound_type__ = None;
                let mut bound_value__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::WindowFrameBoundType => {
                            if window_frame_bound_type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowFrameBoundType"));
                            }
                            window_frame_bound_type__ = Some(map_.next_value::<WindowFrameBoundType>()? as i32);
                        }
                        GeneratedField::BoundValue => {
                            if bound_value__.is_some() {
                                return Err(serde::de::Error::duplicate_field("boundValue"));
                            }
                            bound_value__ = map_.next_value()?;
                        }
                    }
                }
                Ok(WindowFrameBound {
                    window_frame_bound_type: window_frame_bound_type__.unwrap_or_default(),
                    bound_value: bound_value__,
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowFrameBound", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrameBoundType {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::CurrentRow => "CURRENT_ROW",
            Self::Preceding => "PRECEDING",
            Self::Following => "FOLLOWING",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrameBoundType {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "CURRENT_ROW",
            "PRECEDING",
            "FOLLOWING",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrameBoundType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "CURRENT_ROW" => Ok(WindowFrameBoundType::CurrentRow),
                    "PRECEDING" => Ok(WindowFrameBoundType::Preceding),
                    "FOLLOWING" => Ok(WindowFrameBoundType::Following),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for WindowFrameUnits {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Rows => "ROWS",
            Self::Range => "RANGE",
            Self::Groups => "GROUPS",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for WindowFrameUnits {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ROWS",
            "RANGE",
            "GROUPS",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowFrameUnits;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "ROWS" => Ok(WindowFrameUnits::Rows),
                    "RANGE" => Ok(WindowFrameUnits::Range),
                    "GROUPS" => Ok(WindowFrameUnits::Groups),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
impl serde::Serialize for WindowNode {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.input.is_some() {
            len += 1;
        }
        if !self.window_expr.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("datafusion.WindowNode", len)?;
        if let Some(v) = self.input.as_ref() {
            struct_ser.serialize_field("input", v)?;
        }
        if !self.window_expr.is_empty() {
            struct_ser.serialize_field("windowExpr", &self.window_expr)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for WindowNode {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "input",
            "window_expr",
            "windowExpr",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Input,
            WindowExpr,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "input" => Ok(GeneratedField::Input),
                            "windowExpr" | "window_expr" => Ok(GeneratedField::WindowExpr),
                            _ => Err(serde::de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = WindowNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct datafusion.WindowNode")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<WindowNode, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut input__ = None;
                let mut window_expr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Input => {
                            if input__.is_some() {
                                return Err(serde::de::Error::duplicate_field("input"));
                            }
                            input__ = map_.next_value()?;
                        }
                        GeneratedField::WindowExpr => {
                            if window_expr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("windowExpr"));
                            }
                            window_expr__ = Some(map_.next_value()?);
                        }
                    }
                }
                Ok(WindowNode {
                    input: input__,
                    window_expr: window_expr__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("datafusion.WindowNode", FIELDS, GeneratedVisitor)
    }
}
