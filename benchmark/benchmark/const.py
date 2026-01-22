import aiohttp

MILLISECONDS_TO_SECONDS_CONVERSION = 1000
AIOHTTP_TIMEOUT = aiohttp.ClientTimeout(total=6 * 60 * 60)
WARMED = False


Q_STATIC = """SELECT
  other_params, gpu, model, gpu_num, replicas, batch,
  mean_input_tokens, mean_output_tokens,
  TTFT_P90,TPOT_P90,ITL_P90,TTFT_P95,TPOT_P95,ITL_P95,TTFT_P99,TPOT_P99,ITL_P99,mean_TTFT,mean_TPOT,mean_ITL,goodput_percentage,
  prefill_throughput_per_gpu, decode_throughput_per_gpu,
  mean_prefill_throughput, mean_decode_throughput, mean_output_throughput, output_throughput, 
  mean_ITL, mean_E2EL,
  request_throughput, arch,
  case_id, completed, question_label, backend{export_col}
FROM
  summary_data
WHERE
  case_id = '{case_id}'
"""

Q_DYNAMIC = """SELECT
  other_params, gpu, model, gpu_num, replicas, batch,
  mean_input_tokens, mean_output_tokens, num_prompts,
  TTFT_P90,TPOT_P90,ITL_P90,TTFT_P95,TPOT_P95,ITL_P95,TTFT_P99,TPOT_P99,ITL_P99,mean_TTFT,mean_TPOT,mean_ITL,goodput_percentage,request_rate,
  prefill_throughput_per_gpu, decode_throughput_per_gpu,
  mean_prefill_throughput, mean_decode_throughput, mean_output_throughput, output_throughput, 
  mean_ITL, mean_E2EL, 
  request_throughput, arch,
  case_id, completed, question_label, backend{export_col}
FROM
  summary_data
WHERE
  case_id = '{case_id}'
"""