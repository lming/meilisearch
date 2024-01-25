mod extract_docid_word_positions;
mod extract_facet_number_docids;
mod extract_facet_string_docids;
mod extract_fid_docid_facet_values;
mod extract_fid_word_count_docids;
mod extract_geo_points;
mod extract_vector_points;
mod extract_word_docids;
mod extract_word_pair_proximity_docids;
mod extract_word_position_docids;

use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;

use crossbeam_channel::Sender;
use rayon::prelude::*;

use self::extract_docid_word_positions::extract_docid_word_positions;
use self::extract_facet_number_docids::extract_facet_number_docids;
use self::extract_facet_string_docids::extract_facet_string_docids;
use self::extract_fid_docid_facet_values::{extract_fid_docid_facet_values, ExtractedFacetValues};
use self::extract_fid_word_count_docids::extract_fid_word_count_docids;
use self::extract_geo_points::extract_geo_points;
use self::extract_vector_points::{
    extract_embeddings, extract_vector_points, ExtractedVectorPoints,
};
use self::extract_word_docids::extract_word_docids;
use self::extract_word_pair_proximity_docids::extract_word_pair_proximity_docids;
use self::extract_word_position_docids::extract_word_position_docids;
use super::helpers::{as_cloneable_grenad, CursorClonableMmap, GrenadParameters};
use super::{helpers, TypedChunk};
use crate::proximity::ProximityPrecision;
use crate::vector::EmbeddingConfigs;
use crate::{FieldId, FieldsIdsMap, Result};

/// Extract data for each databases from obkv documents in parallel.
/// Send data in grenad file over provided Sender.
#[allow(clippy::too_many_arguments)]
pub(crate) fn data_from_obkv_documents(
    original_obkv_chunks: impl Iterator<Item = Result<grenad::Reader<BufReader<File>>>> + Send,
    flattened_obkv_chunks: impl Iterator<Item = Result<grenad::Reader<BufReader<File>>>> + Send,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    searchable_fields: Option<HashSet<FieldId>>,
    faceted_fields: HashSet<FieldId>,
    primary_key_id: FieldId,
    geo_fields_ids: Option<(FieldId, FieldId)>,
    field_id_map: FieldsIdsMap,
    stop_words: Option<fst::Set<Vec<u8>>>,
    allowed_separators: Option<&[&str]>,
    dictionary: Option<&[&str]>,
    max_positions_per_attributes: Option<u32>,
    exact_attributes: HashSet<FieldId>,
    proximity_precision: ProximityPrecision,
    embedders: EmbeddingConfigs,
) -> Result<()> {
    puffin::profile_function!();

    let (original_pipeline_result, flattened_pipeline_result): (Result<_>, Result<_>) = rayon::join(
        || {
            original_obkv_chunks
                .par_bridge()
                .map(|original_documents_chunk| {
                    send_original_documents_data(
                        original_documents_chunk,
                        indexer,
                        lmdb_writer_sx.clone(),
                        field_id_map.clone(),
                        embedders.clone(),
                    )
                })
                .collect::<Result<()>>()
        },
        || {
            flattened_obkv_chunks
                .par_bridge()
                .map(|flattened_obkv_chunks| {
                    send_and_extract_flattened_documents_data(
                        flattened_obkv_chunks,
                        indexer,
                        lmdb_writer_sx.clone(),
                        &searchable_fields,
                        &faceted_fields,
                        primary_key_id,
                        geo_fields_ids,
                        &stop_words,
                        &allowed_separators,
                        &dictionary,
                        max_positions_per_attributes,
                    )
                })
                .map(|result| {
                    if let Ok((
                        ref docid_word_positions_chunk,
                        (ref fid_docid_facet_numbers_chunk, ref fid_docid_facet_strings_chunk),
                    )) = result
                    {
                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            lmdb_writer_sx.clone(),
                            extract_fid_word_count_docids,
                            TypedChunk::FieldIdWordCountDocids,
                            "field-id-wordcount-docids",
                        );

                        let exact_attributes = exact_attributes.clone();
                        run_extraction_task::<
                            _,
                            _,
                            (
                                grenad::Reader<BufReader<File>>,
                                grenad::Reader<BufReader<File>>,
                                grenad::Reader<BufReader<File>>,
                            ),
                        >(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            lmdb_writer_sx.clone(),
                            move |doc_word_pos, indexer| {
                                extract_word_docids(doc_word_pos, indexer, &exact_attributes)
                            },
                            |(
                                word_docids_reader,
                                exact_word_docids_reader,
                                word_fid_docids_reader,
                            )| {
                                TypedChunk::WordDocids {
                                    word_docids_reader,
                                    exact_word_docids_reader,
                                    word_fid_docids_reader,
                                }
                            },
                            "word-docids",
                        );

                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            docid_word_positions_chunk.clone(),
                            indexer,
                            lmdb_writer_sx.clone(),
                            extract_word_position_docids,
                            TypedChunk::WordPositionDocids,
                            "word-position-docids",
                        );

                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            fid_docid_facet_strings_chunk.clone(),
                            indexer,
                            lmdb_writer_sx.clone(),
                            extract_facet_string_docids,
                            TypedChunk::FieldIdFacetStringDocids,
                            "field-id-facet-string-docids",
                        );

                        run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                            fid_docid_facet_numbers_chunk.clone(),
                            indexer,
                            lmdb_writer_sx.clone(),
                            extract_facet_number_docids,
                            TypedChunk::FieldIdFacetNumberDocids,
                            "field-id-facet-number-docids",
                        );

                        if proximity_precision == ProximityPrecision::ByWord {
                            run_extraction_task::<_, _, grenad::Reader<BufReader<File>>>(
                                docid_word_positions_chunk.clone(),
                                indexer,
                                lmdb_writer_sx.clone(),
                                extract_word_pair_proximity_docids,
                                TypedChunk::WordPairProximityDocids,
                                "word-pair-proximity-docids",
                            );
                        }
                    }

                    Ok(())
                })
                .collect::<Result<()>>()
        },
    );

    original_pipeline_result.and(flattened_pipeline_result)
}

/// Spawn a new task to extract data for a specific DB using extract_fn.
/// Generated grenad chunks are merged using the merge_fn.
/// The result of merged chunks is serialized as TypedChunk using the serialize_fn
/// and sent into lmdb_writer_sx.
fn run_extraction_task<FE, FS, M>(
    chunk: grenad::Reader<CursorClonableMmap>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    extract_fn: FE,
    serialize_fn: FS,
    name: &'static str,
) where
    FE: Fn(grenad::Reader<CursorClonableMmap>, GrenadParameters) -> Result<M>
        + Sync
        + Send
        + 'static,
    FS: Fn(M) -> TypedChunk + Sync + Send + 'static,
    M: Send,
{
    rayon::spawn(move || {
        puffin::profile_scope!("extract_chunk", name);
        match extract_fn(chunk, indexer) {
            Ok(chunk) => {
                let _ = lmdb_writer_sx.send(Ok(serialize_fn(chunk)));
            }
            Err(e) => {
                let _ = lmdb_writer_sx.send(Err(e));
            }
        }
    })
}

/// Extract chunked data and send it into lmdb_writer_sx sender:
/// - documents
fn send_original_documents_data(
    original_documents_chunk: Result<grenad::Reader<BufReader<File>>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    field_id_map: FieldsIdsMap,
    embedders: EmbeddingConfigs,
) -> Result<()> {
    let original_documents_chunk =
        original_documents_chunk.and_then(|c| unsafe { as_cloneable_grenad(&c) })?;

    let documents_chunk_cloned = original_documents_chunk.clone();
    let lmdb_writer_sx_cloned = lmdb_writer_sx.clone();
    rayon::spawn(move || {
        for (name, (embedder, prompt)) in embedders {
            let result = extract_vector_points(
                documents_chunk_cloned.clone(),
                indexer,
                &field_id_map,
                &prompt,
                &name,
            );
            match result {
                Ok(ExtractedVectorPoints { manual_vectors, remove_vectors, prompts }) => {
                    let embeddings = match extract_embeddings(prompts, indexer, embedder.clone()) {
                        Ok(results) => Some(results),
                        Err(error) => {
                            let _ = lmdb_writer_sx_cloned.send(Err(error));
                            None
                        }
                    };

                    if !(remove_vectors.is_empty()
                        && manual_vectors.is_empty()
                        && embeddings.as_ref().map_or(true, |e| e.is_empty()))
                    {
                        let _ = lmdb_writer_sx_cloned.send(Ok(TypedChunk::VectorPoints {
                            remove_vectors,
                            embeddings,
                            expected_dimension: embedder.dimensions(),
                            manual_vectors,
                            embedder_name: name,
                        }));
                    }
                }

                Err(error) => {
                    let _ = lmdb_writer_sx_cloned.send(Err(error));
                }
            }
        }
    });

    // TODO: create a custom internal error
    lmdb_writer_sx.send(Ok(TypedChunk::Documents(original_documents_chunk))).unwrap();
    Ok(())
}

/// Extract chunked data and send it into lmdb_writer_sx sender:
/// - documents_ids
/// - docid_word_positions
/// - docid_fid_facet_numbers
/// - docid_fid_facet_strings
/// - docid_fid_facet_exists
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
fn send_and_extract_flattened_documents_data(
    flattened_documents_chunk: Result<grenad::Reader<BufReader<File>>>,
    indexer: GrenadParameters,
    lmdb_writer_sx: Sender<Result<TypedChunk>>,
    searchable_fields: &Option<HashSet<FieldId>>,
    faceted_fields: &HashSet<FieldId>,
    primary_key_id: FieldId,
    geo_fields_ids: Option<(FieldId, FieldId)>,
    stop_words: &Option<fst::Set<Vec<u8>>>,
    allowed_separators: &Option<&[&str]>,
    dictionary: &Option<&[&str]>,
    max_positions_per_attributes: Option<u32>,
) -> Result<(
    grenad::Reader<CursorClonableMmap>,
    (grenad::Reader<CursorClonableMmap>, grenad::Reader<CursorClonableMmap>),
)> {
    let flattened_documents_chunk =
        flattened_documents_chunk.and_then(|c| unsafe { as_cloneable_grenad(&c) })?;

    if let Some(geo_fields_ids) = geo_fields_ids {
        let documents_chunk_cloned = flattened_documents_chunk.clone();
        let lmdb_writer_sx_cloned = lmdb_writer_sx.clone();
        rayon::spawn(move || {
            let result =
                extract_geo_points(documents_chunk_cloned, indexer, primary_key_id, geo_fields_ids);
            let _ = match result {
                Ok(geo_points) => lmdb_writer_sx_cloned.send(Ok(TypedChunk::GeoPoints(geo_points))),
                Err(error) => lmdb_writer_sx_cloned.send(Err(error)),
            };
        });
    }

    let (docid_word_positions_chunk, fid_docid_facet_values_chunks): (Result<_>, Result<_>) =
        rayon::join(
            || {
                let (docid_word_positions_chunk, script_language_pair) =
                    extract_docid_word_positions(
                        flattened_documents_chunk.clone(),
                        indexer,
                        searchable_fields,
                        stop_words.as_ref(),
                        *allowed_separators,
                        *dictionary,
                        max_positions_per_attributes,
                    )?;

                // send docid_word_positions_chunk to DB writer
                let docid_word_positions_chunk =
                    unsafe { as_cloneable_grenad(&docid_word_positions_chunk)? };

                let _ =
                    lmdb_writer_sx.send(Ok(TypedChunk::ScriptLanguageDocids(script_language_pair)));

                Ok(docid_word_positions_chunk)
            },
            || {
                let ExtractedFacetValues {
                    fid_docid_facet_numbers_chunk,
                    fid_docid_facet_strings_chunk,
                    fid_facet_is_null_docids_chunk,
                    fid_facet_is_empty_docids_chunk,
                    fid_facet_exists_docids_chunk,
                } = extract_fid_docid_facet_values(
                    flattened_documents_chunk.clone(),
                    indexer,
                    faceted_fields,
                    geo_fields_ids,
                )?;

                // send fid_docid_facet_numbers_chunk to DB writer
                let fid_docid_facet_numbers_chunk =
                    unsafe { as_cloneable_grenad(&fid_docid_facet_numbers_chunk)? };

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdDocidFacetNumbers(
                    fid_docid_facet_numbers_chunk.clone(),
                )));

                // send fid_docid_facet_strings_chunk to DB writer
                let fid_docid_facet_strings_chunk =
                    unsafe { as_cloneable_grenad(&fid_docid_facet_strings_chunk)? };

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdDocidFacetStrings(
                    fid_docid_facet_strings_chunk.clone(),
                )));

                let _ = lmdb_writer_sx
                    .send(Ok(TypedChunk::FieldIdFacetIsNullDocids(fid_facet_is_null_docids_chunk)));

                let _ = lmdb_writer_sx.send(Ok(TypedChunk::FieldIdFacetIsEmptyDocids(
                    fid_facet_is_empty_docids_chunk,
                )));

                let _ = lmdb_writer_sx
                    .send(Ok(TypedChunk::FieldIdFacetExistsDocids(fid_facet_exists_docids_chunk)));

                Ok((fid_docid_facet_numbers_chunk, fid_docid_facet_strings_chunk))
            },
        );

    Ok((docid_word_positions_chunk?, fid_docid_facet_values_chunks?))
}
